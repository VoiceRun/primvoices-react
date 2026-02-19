/**
 * WebSocketClient
 * 
 * A client for handling WebSocket communications with the PrimVoices TTS API.
 * 
 * This client supports:
 * - Establishing WebSocket connections
 * - Sending audio data from microphone
 * - Receiving and processing audio responses
 * - Managing the lifecycle of an audio conversation
 */

import { v4 as uuidv4 } from "uuid";
import * as alawmulaw from "alawmulaw";

import { Logger } from "./Logger";

const logger = new Logger();

// Types for configuration and callbacks
export interface WebSocketClientConfig {
  agentId: string;
  functionId?: string;
  environment?: string;
  strategy?: "cascade" | "sts";
  logLevel?: "DEBUG" | "INFO" | "WARN" | "ERROR";
  serverUrl?: string;
  apiUrl?: string;
  customParameters?: Record<string, string>;
  canary?: boolean;
  origin?: "debugger" | "web";
}

interface Mark {
  mark: {
    name: string;
  };
}

interface QueuedAudioItem {
  data: Float32Array;
  sampleRate: number;
  mark?: Mark; 
}

export interface DebugMessage {
  type: string;
  turn: number;
  name: string;
  data: Record<string, unknown>;
}

/**
 * Audio statistics containing level and speech detection information
 * isPlayback indicates whether these stats are for playback audio (true) or microphone input (false)
 */
export interface AudioStats {
  level: number;
  isSpeaking: boolean;
  isPlayback?: boolean; // Indicates if the stats are for playback or microphone audio
}

export type AudioDataCallback = (audioData: Float32Array) => void;
export type AudioStatsCallback = (stats: AudioStats) => void;
export type DebugMessageCallback = (messages: DebugMessage[]) => void;
export type StatusCallback = () => void;

export class WebSocketClient {
  private socket: WebSocket | null = null;
  private audioContext: AudioContext | null = null;
  private mediaStream: MediaStream | null = null;
  private microphoneSource: MediaStreamAudioSourceNode | null = null;
  private audioWorklet: AudioWorkletNode | null = null;
  private analyser: AnalyserNode | null = null;
  private audioQueue: QueuedAudioItem[] = [];
  private currentAudioSource: AudioBufferSourceNode | null = null;
  private debugQueue: DebugMessage[] = [];
  private workletInitialized = false;
  private isListening = false;
  private isConnected = false;
  private isPlaying = false;
  private callSid = "";
  private streamSid = "";
  private config: WebSocketClientConfig;
  private speechDetected = false;
  private statsInterval: number | null = null;
  private initialAgentId: string;
  private initialEnvironment?: string;
  private redirected = false;
  
  // Playback scheduling
  private scheduledSources: AudioBufferSourceNode[] = [];
  private nextPlaybackTime = 0; // audioContext time for next start
  private scheduledMarkTimers: number[] = [];
  private scheduleTimer: number | null = null;
  private readonly minPrebufferSeconds = 0.25; // initial prebuffer to avoid choppiness
  private readonly scheduleHorizonSeconds = 1.0; // keep at least this much scheduled ahead

  // Callbacks
  private onConnectionOpen: StatusCallback | null = null;
  private onConnectionClose: StatusCallback | null = null;
  private onConnectionError: StatusCallback | null = null;
  private onStartListening: StatusCallback | null = null;
  private onStopListening: StatusCallback | null = null;
  private onPlayStart: StatusCallback | null = null;
  private onPlayStop: StatusCallback | null = null;
  private onAudioStats: AudioStatsCallback | null = null;
  private onDebugMessage: DebugMessageCallback | null = null;
  private lastRedirectKey: string | null = null;
  
  constructor(config: WebSocketClientConfig) {
    this.config = {
      apiUrl: "https://api.primvoices.com",
      logLevel: "ERROR",
      customParameters: {},
      ...config,
    };

    // Capture initial target so we can restore it after a redirect
    this.initialAgentId = this.config.agentId;
    this.initialEnvironment = this.config.environment;

    logger.setLogLevel(this.config.logLevel || "ERROR");
    
    // Initialize the audio context
    this.initAudioContext();
  }

  /**
   * Set callbacks for different events
   */
  public setCallbacks({
    onOpen,
    onClose,
    onError,
    onListeningStart,
    onListeningStop,
    onAudioStart,
    onAudioStop,
    onAudioStats,
    onDebugMessage,
  }: {
    onOpen?: StatusCallback;
    onClose?: StatusCallback;
    onError?: StatusCallback;
    onListeningStart?: StatusCallback;
    onListeningStop?: StatusCallback;
    onAudioStart?: StatusCallback;
    onAudioStop?: StatusCallback;
    onAudioStats?: AudioStatsCallback;
    onDebugMessage?: DebugMessageCallback;
  }): void {
    this.onConnectionOpen = onOpen || null;
    this.onConnectionClose = onClose || null;
    this.onConnectionError = onError || null;
    this.onStartListening = onListeningStart || null;
    this.onStopListening = onListeningStop || null;
    this.onPlayStart = onAudioStart || null;
    this.onPlayStop = onAudioStop || null;
    this.onAudioStats = onAudioStats || null;
    this.onDebugMessage = onDebugMessage || null;
  }

  public async getAgentConfiguration(): Promise<{ url: string, parameters: Record<string, string> }> {
    const queryParams = new URLSearchParams();
    queryParams.set("inputType", "mic");
    queryParams.set("environment", this.config.environment || "");

    if (this.config.customParameters) {
      Object.entries(this.config.customParameters).forEach(([key, value]) => {
        queryParams.set(`custom_${key}`, value);
      });
    }
    
    const response = await fetch(`${this.config.apiUrl}/v1/agents/${this.config.agentId}/call?${queryParams.toString()}`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
      },
      body: JSON.stringify({}),
    });

    if (!response.ok) {
      const errorData = await response.json().catch(() => ({}));
      throw new Error(errorData.error || `Request failed with status ${response.status}`);
    }

    const responseJson = await response.json();

    return responseJson.data;
  }

  /**
   * Initialize the WebSocket connection
   */
  public async connect(): Promise<void> {   
    // Generate unique IDs for this session
    this.callSid = uuidv4();
    this.streamSid = uuidv4();
    
    // Remove any debug messages from the previous session
    this.clearDebugQueue();
    
    // Close existing connection if there is one
    if (this.socket) {
      this.socket.close();
    }

    if (!this.config.agentId) {
      throw new Error("agentId is required");
    }

    if (!this.config.serverUrl) {
      const agentConfiguration = await this.getAgentConfiguration();

      logger.info("[WebSocketClient] Agent configuration:", agentConfiguration);

      this.config.serverUrl = agentConfiguration.url;
      this.config.customParameters = agentConfiguration.parameters;
    }
    
    // Construct the WebSocket URL with canary parameter if enabled
    let wsUrl = this.config.serverUrl;
    if (this.config.canary) {
      const separator = wsUrl.includes('?') ? '&' : '?';
      wsUrl = `${wsUrl}${separator}canary=true`;
    }
    
    // Create and setup new WebSocket connection
    this.socket = new WebSocket(wsUrl);

    logger.info(`[WebSocketClient] Connecting to ${wsUrl}`);
    logger.info(`[WebSocketClient] Session IDs: call=${this.callSid}, stream=${this.streamSid}`);

    return new Promise((resolve, reject) => {        
      if (!this.socket) {
        reject(new Error("WebSocket not initialized"));
        return;
      }

      this.socket.onopen = () => {
        this.isConnected = true;
        
        // Build custom parameters with enforced essentials
        const params = {
          ...(this.config.customParameters || {}),
          agentId: this.config.agentId,
          environment: this.config.environment || "",
          inputType: "mic",
          origin: this.config.origin || "",
        } as Record<string, string>;
        
        // Send start message following the format in audio.ts
        const startMessage = {
          start: {
            streamSid: this.streamSid,
            callSid: this.callSid,
            customParameters: params,
          },
        };
        
        // Send the start message
        this.socket?.send(JSON.stringify(startMessage));
      
        logger.info("[WebSocketClient] Connection established");
        logger.info("[WebSocketClient] Sent start message:", startMessage);
        
        if (this.onConnectionOpen) {
          this.onConnectionOpen();
        }

        resolve();
      };
      
      this.socket.onclose = () => {
        this.isConnected = false;

        logger.info("[WebSocketClient] Connection closed");

        if (this.onConnectionClose) {
          this.onConnectionClose();
        }

        this.stopListening();
      };
      
      this.socket.onerror = (error) => {
        logger.error("[WebSocketClient] WebSocket error:", error);

        if (this.onConnectionError) {
          this.onConnectionError();
        }
      };
      
      this.socket.onmessage = (event) => {
        try {
          const data = JSON.parse(event.data);

          // Handle different message types
          if (data.event === "media") {
            this.handleAudioMessage(data);
          } else if (data.event === "clear") {
            this.handleClearMessage(data);
          } else if (data.event === "mark") {
            this.handleMarkMessage(data);
          } else if (data.event === "debug") {
            this.handleDebugMessage(data);
          } else if (data.event === "control") {
            this.handleControlMessage(data);
          }
        } catch (error) {
          logger.error("[WebSocketClient] Error parsing message:", error);
        }
      };
    });
  }

  /**
   * Handle audio data received from the server
   */
  private handleAudioMessage(data: any): void {
    if (!data.media || !data.media.payload) return;
    
    // Decode base64 audio
    const base64 = data.media.payload;
    const binary = atob(base64);
    const bytes = new Uint8Array(binary.length);
    
    for (let i = 0; i < binary.length; i++) {
      bytes[i] = binary.charCodeAt(i);
    }
    
    // Always assume 24kHz audio (regardless of what format might be specified)
    // Make sure we have even number of bytes for 16-bit samples
    let int16Data: Int16Array;
    
    if (bytes.length % 2 !== 0) {
      // Create a properly aligned copy with even length
      const alignedBuffer = new ArrayBuffer(bytes.length + (bytes.length % 2));
      new Uint8Array(alignedBuffer).set(bytes);
      int16Data = new Int16Array(alignedBuffer);
    } else {
      // Buffer is already properly sized
      const alignedBuffer = new ArrayBuffer(bytes.length);
      new Uint8Array(alignedBuffer).set(bytes);
      int16Data = new Int16Array(alignedBuffer);
    }
    
    // Convert to Float32Array for Web Audio API
    const floatData = new Float32Array(int16Data.length);
    for (let i = 0; i < int16Data.length; i++) {
      floatData[i] = int16Data[i] / 32768.0;  // Scale to -1.0 to 1.0
    }
    
    // Add to audio queue for playback with 24kHz sample rate
    // Include mark if it exists in the data
    this.addToAudioQueue(floatData, 24000);
  }

  private handleClearMessage(data: any): void {
    logger.info("[WebSocketClient] Received clear message:", data);

    // Send all mark events to the server in the order they were received.
    // This replicates the behavior of Twilio's SDK on clear.
    this.audioQueue.forEach((item) => {
      if (item.mark && this.socket && this.socket.readyState === WebSocket.OPEN) {
        this.socket.send(JSON.stringify({ 
          event: "mark", 
          mark: item.mark,
          streamSid: this.streamSid,
        }));

        logger.debug(`[WebSocketClient] Sent mark event: ${item}`);
      }
    });

    // Clear the audio queue.
    this.clearAudioQueue();
  }

  /**
   * Handle mark events received from the server
   * These marks will be associated with the next audio chunk received
   */
  private handleMarkMessage(data: Mark): void {
    logger.info(`[WebSocketClient] Received mark event: ${data.mark?.name}`);

    const mark = {
      mark: data.mark,
    };

    this.addToAudioQueue(new Float32Array(0), 24000, mark);
  }

  /**
   * Handle debug messages received from the server
   */
  private handleDebugMessage(data: any): void {
    logger.info("[WebSocketClient] Received debug message:", data);

    this.debugQueue.push(data);

    if (this.onDebugMessage) {
      this.onDebugMessage(this.debugQueue);
    }
  }

  private async handleControlMessage(data: any): Promise<void> {
    try {
      if (data.name === "redirect") {
        const agentId = data?.data?.agentId as string;
        const environment = (data?.data?.environment as string) || this.config.environment || "";
        
        if (!agentId) return;

        // Idempotence: skip if already targeting the same agent/environment
        const key = `${agentId}|${environment || ""}`;
        const alreadyAtTarget =
          agentId === this.config.agentId &&
          (environment || "") === (this.config.environment || "") &&
          this.socket &&
          (this.socket.readyState === WebSocket.OPEN || this.socket.readyState === WebSocket.CONNECTING);

        if (alreadyAtTarget || this.lastRedirectKey === key) {
          return;
        }
        
        // Quiesce current session
        this.stopListening();
        this.clearAudioQueue();
        this.socket?.close();

        // Update config for the new agent
        this.config.agentId = agentId;
        this.config.environment = environment || this.config.environment;
        this.redirected = true; // Mark that a redirect occurred
        this.lastRedirectKey = key;
        
        // Reconnect to the new agent
        this.connect()
          .then(() => this.startListening())
          .catch((error) => logger.error("[WebSocketClient] Error during redirect reconnect:", error));
      }
    } catch (e) {
      logger.error("[WebSocketClient] Error handling control message:", e);
    }
  }

  /**
   * Initialize the audio context and related components
   */
  private async initAudioContext(): Promise<void> {
    try {
      this.audioContext = new (window.AudioContext || (window as any).webkitAudioContext)();
      this.analyser = this.audioContext.createAnalyser();
      this.analyser.fftSize = 256;
      
      // Register the audio worklet for processing microphone input
      if (this.audioContext.audioWorklet && !this.workletInitialized) {
        const workletCode = `
          class AudioProcessor extends AudioWorkletProcessor {
            constructor() {
              super();
              this.port.onmessage = this.handleMessage.bind(this);
            }
          
            handleMessage(event) {
              if (event.data.command === "stop") {
                // Handle stop command if needed
              }
            }
          
            process(inputs, outputs, parameters) {
              // Get input data from the microphone
              const input = inputs[0];
              if (input.length > 0 && input[0].length > 0) {
                const audioData = input[0];
                
                // Send audio data to the main thread
                this.port.postMessage({
                  audioData: audioData
                });
              }
              
              // Return true to keep the processor alive
              return true;
            }
          }
          
          registerProcessor("audio-processor", AudioProcessor);
        `;
        
        const blob = new Blob([workletCode], { type: "application/javascript" });
        const blobURL = URL.createObjectURL(blob);
        
        await this.audioContext.audioWorklet.addModule(blobURL);
        URL.revokeObjectURL(blobURL);
        
        this.workletInitialized = true;
        
        logger.info("[WebSocketClient] Audio worklet initialized");
      }
    } catch (error) {
      logger.error("[WebSocketClient] Error initializing audio context:", error);
    }
  }

  /**
   * Start capturing audio from the microphone and sending it to the server
   */
  public async startListening(): Promise<void> {
    if (this.isListening || !this.isConnected) return;
    
    try {
      // Resume audio context if it"s suspended
      if (this.audioContext?.state === "suspended") {
        await this.audioContext.resume();
      }
      
      // Request microphone access
      this.mediaStream = await navigator.mediaDevices.getUserMedia({ 
        audio: true,
        video: false 
      });
      
      if (!this.audioContext) {
        throw new Error("Audio context not initialized");
      }
      
      // Create microphone source and connect it to the audio graph
      this.microphoneSource = this.audioContext.createMediaStreamSource(this.mediaStream);
      
      if (this.workletInitialized) {
        // Use the audio worklet for processing
        this.audioWorklet = new AudioWorkletNode(this.audioContext, "audio-processor");
        this.microphoneSource.connect(this.audioWorklet);
        
        if (this.analyser) {
          this.microphoneSource.connect(this.analyser);
        }
        
        // Handle audio data from the worklet
        this.audioWorklet.port.onmessage = (event) => {
          if (event.data.audioData && this.isListening) {
            this.processAudioData(event.data.audioData);
          }
        };
      } else {
        // Fallback to ScriptProcessorNode (deprecated but more widely supported)
        logger.warn("[WebSocketClient] Using ScriptProcessorNode fallback");

        const bufferSize = 4096;
        const scriptProcessor = this.audioContext.createScriptProcessor(bufferSize, 1, 1);
        this.microphoneSource.connect(scriptProcessor);
        scriptProcessor.connect(this.audioContext.destination);
        
        if (this.analyser) {
          this.microphoneSource.connect(this.analyser);
        }
        
        scriptProcessor.onaudioprocess = (event) => {
          if (this.isListening) {
            const inputData = event.inputBuffer.getChannelData(0);
            this.processAudioData(inputData);
          }
        };
      }
      
      this.isListening = true;
      
      // Start audio stats monitoring
      this.startAudioStatsMonitoring();
      
      if (this.onStartListening) {
        this.onStartListening();
      }
      
      logger.info("[WebSocketClient] Started listening");
    } catch (error) {
      logger.error("[WebSocketClient] Error starting microphone:", error);

      this.isListening = false;
    }
  }

  /**
   * Stop capturing audio from the microphone
   */
  public stopListening(): void {
    if (!this.isListening) return;
    
    // Stop all media tracks
    if (this.mediaStream) {
      this.mediaStream.getTracks().forEach(track => track.stop());
      this.mediaStream = null;
    }
    
    // Disconnect audio nodes
    if (this.microphoneSource) {
      this.microphoneSource.disconnect();
      this.microphoneSource = null;
    }
    
    if (this.audioWorklet) {
      this.audioWorklet.disconnect();
      this.audioWorklet = null;
    }
    
    this.isListening = false;
    
    // Stop audio stats monitoring
    this.stopAudioStatsMonitoring();
    
    if (this.onStopListening) {
      this.onStopListening();
    }
    
    logger.info("[WebSocketClient] Stopped listening");
  }

  /**
   * Send a text message to the server
   */
  public sendTextEvent(text: string): void {
    if (!this.socket || this.socket.readyState !== WebSocket.OPEN) {
      logger.error("[WebSocketClient] Cannot send text message: WebSocket is not open");
      return;
    }

    this.socket.send(JSON.stringify({ event: "text", text }));
  }

  /**
   * Close the WebSocket connection and clean up resources
   */
  public disconnect(): void {
    this.stopListening();
    this.clearAudioQueue();
    this.stopAudioStatsMonitoring();

    if (this.socket) {
      this.socket.close();
      this.socket = null;
    }

    this.isConnected = false;
    // Clear last redirect idempotence key on explicit disconnect
    this.lastRedirectKey = null;

    // Clear cached serverUrl and parameters so next connect() fetches fresh routing
    this.config.serverUrl = undefined;
    this.config.customParameters = {};

    // If we had redirected, restore initial agent/environment so the next connect
    // returns to the original target unless app overrides explicitly.
    if (this.redirected) {
      this.config.agentId = this.initialAgentId;
      this.config.environment = this.initialEnvironment;
      this.config.customParameters = {
        ...(this.config.customParameters || {}),
        agentId: this.config.agentId,
        environment: this.config.environment || "",
        inputType: "mic",
      };
      this.redirected = false;
      logger.info(`[WebSocketClient] Restored initial agent after redirect: agent=${this.config.agentId} env=${this.config.environment}`);
    }

    if (this.onConnectionClose) {
      this.onConnectionClose();
    }
  }

  /**
   * Process captured audio data and send it to the server
   */
  private processAudioData(inputData: Float32Array): void {
    if (!this.socket || this.socket.readyState !== WebSocket.OPEN) return;
    
    try {
      // Sample rate of the incoming audio data
      const sampleRate = this.audioContext?.sampleRate || 48000;
      
      // Twilio expects 16kHz μ-law encoded audio, as in audio.ts
      const targetSampleRate = 16000;
      
      // Check if there"s sound in the input data
      const hasSound = inputData.some((value) => Math.abs(value) > 0.01);
      logger.debug(`[WebSocketClient] Processing audio frame: ${inputData.length} samples at ${sampleRate}Hz ${hasSound ? "(has sound)" : "(silent)"}`);
      
      // Downsample to 16kHz as in audio.ts
      // This directly returns Int16Array scaled to 16-bit range
      const int16Data = this.downsampleBuffer(inputData, sampleRate, targetSampleRate);
      
      logger.debug(`[WebSocketClient] Downsampled to ${int16Data.length} samples at ${targetSampleRate}Hz`);
      
      // Convert PCM to μ-law as in audio.ts
      const muLawData = alawmulaw.mulaw.encode(int16Data);
      
      logger.debug(`[WebSocketClient] Converted to μ-law format: ${muLawData.length} bytes`);
      
      // Encode to base64
      const base64Data = this.arrayBufferToBase64(muLawData.buffer);
      
      // Send via WebSocket - match format in audio.ts exactly
      const message = {
        event: "media",
        streamSid: this.streamSid,
        media: {
          payload: base64Data
        },
      };
      
      this.socket.send(JSON.stringify(message));
      
      logger.debug(`[WebSocketClient] Sent μ-law encoded audio: ${base64Data.length} base64 chars`);
    } catch (error) {
      logger.error("[WebSocketClient] Error processing or sending audio:", error);
    }
  }

  /**
   * Convert array buffer to base64
   */
  private arrayBufferToBase64(buffer: ArrayBufferLike): string {
    const bytes = new Uint8Array(buffer);

    let binary = "";
    for (let i = 0; i < bytes.byteLength; i++) {
      binary += String.fromCharCode(bytes[i]);
    }

    return btoa(binary);
  }

  /**
   * Downsample audio buffer - matches audio.ts implementation
   */
  private downsampleBuffer(buffer: Float32Array, originalSampleRate: number, targetSampleRate: number): Int16Array {
    if (targetSampleRate > originalSampleRate) {
      throw new Error("downsampling rate should be lower than original sample rate");
    }
    
    const sampleRateRatio = originalSampleRate / targetSampleRate;
    const newLength = Math.round(buffer.length / sampleRateRatio);
    const result = new Int16Array(newLength);

    let lastIndex = 0;
    for (let i = 0; i < newLength; i++) {
      const nextIndex = Math.round((i + 1) * sampleRateRatio);
      let accum = 0,
        count = 0;
      
      for (let j = lastIndex; j < nextIndex && j < buffer.length; j++) {
        accum += buffer[j];
        count++;
      }
      
      result[i] = count > 0 ? Math.round((accum / count) * 32767) : 0;
      lastIndex = nextIndex;
    }
    
    return result;
  }

  /**
   * Add audio data to the playback queue
   */
  private addToAudioQueue(audioData: Float32Array, sampleRate = 16000, mark?: Mark): void {
    this.audioQueue.push({ data: audioData, sampleRate, mark });
    this.schedulePlayback();
  }

  /**
   * Clear the audio playback queue and stop any current playback
   */
  private clearAudioQueue(): void {
    this.audioQueue = [];
    
    // Stop any currently playing source
    if (this.currentAudioSource) {
      try { this.currentAudioSource.stop(); } catch (e) {}
      try { this.currentAudioSource.disconnect(); } catch (e) {}
      this.currentAudioSource = null;
    }
    
    // Stop all scheduled sources
    this.scheduledSources.forEach((source) => {
      try { source.stop(); } catch (e) {}
      try { source.disconnect(); } catch (e) {}
    });
    this.scheduledSources = [];
    
    // Clear any scheduled mark timers
    this.scheduledMarkTimers.forEach((id) => clearTimeout(id));
    this.scheduledMarkTimers = [];
    
    // Stop scheduler
    if (this.scheduleTimer) {
      clearInterval(this.scheduleTimer);
      this.scheduleTimer = null;
    }
    
    // Reset scheduling cursor
    this.nextPlaybackTime = 0;
    
    this.isPlaying = false;
  }

  /**
   * Clear the debug queue
   */
  private clearDebugQueue(): void {
    this.debugQueue = [];

    if (this.onDebugMessage) {
      this.onDebugMessage([]);
    }
  }

  /**
   * Play the next audio chunk in the queue
   */
  private playNextInQueue(): void {
    if (!this.audioContext || this.audioQueue.length === 0) {
      this.isPlaying = false;
      
      // If we"re not listening from the microphone, stop audio monitoring
      if (!this.isListening) {
        this.stopAudioStatsMonitoring();
      }
      
      if (this.onPlayStop) {
        this.onPlayStop();
      }
      
      return;
    }
    
    // Legacy method now delegates to scheduler for gapless playback
    this.schedulePlayback();
  }

  /**
   * Compute total buffered audio seconds currently in the queue (excluding marks)
   */
  private getBufferedSecondsInQueue(): number {
    return this.audioQueue.reduce((sum, item) => {
      if (item.mark || item.data.length === 0 || item.sampleRate <= 0) return sum;
      return sum + item.data.length / item.sampleRate;
    }, 0);
  }

  /**
   * Ensure playback is started and enough audio is scheduled ahead to avoid gaps
   */
  private schedulePlayback(): void {
    if (!this.audioContext) return;

    // If not currently playing, wait for prebuffer before starting
    if (!this.isPlaying) {
      const buffered = this.getBufferedSecondsInQueue();
      if (buffered <= 0) return;

      if (buffered < this.minPrebufferSeconds) {
        // Not enough to start yet
        return;
      }

      // Start playback cursor with a small delay to give headroom
      const startAt = Math.max(this.audioContext.currentTime + 0.02, this.audioContext.currentTime + this.minPrebufferSeconds);
      this.nextPlaybackTime = startAt;
      this.isPlaying = true;

      // Start monitoring and fire start callback
      if (!this.statsInterval) {
        this.startAudioStatsMonitoring();
      }
      if (this.onPlayStart) {
        this.onPlayStart();
      }
      
      // Start lightweight scheduler to keep horizon filled
      if (!this.scheduleTimer) {
        this.scheduleTimer = window.setInterval(() => {
          this.scheduleFromQueue();
        }, 50);
      }
    }

    // Schedule from queue up to the horizon
    this.scheduleFromQueue();
  }

  /**
   * Schedule queued items contiguously at nextPlaybackTime up to a horizon
   */
  private scheduleFromQueue(): void {
    if (!this.audioContext) return;

    // Keep scheduling while we have items and we're within the horizon
    while (this.audioQueue.length > 0) {
      const timeAhead = this.nextPlaybackTime - this.audioContext.currentTime;
      if (timeAhead > this.scheduleHorizonSeconds) break;

      const item = this.audioQueue.shift();
      if (!item) break;

      const { data, sampleRate, mark } = item;

      if (mark) {
        // Schedule mark to be sent when playback reaches this point
        const delayMs = Math.max(0, (this.nextPlaybackTime - this.audioContext.currentTime) * 1000);
        const id = window.setTimeout(() => {
          if (this.socket && this.socket.readyState === WebSocket.OPEN) {
            this.socket.send(JSON.stringify({
              event: "mark",
              streamSid: this.streamSid,
              mark: mark.mark
            }));
            logger.debug(`[WebSocketClient] Sent mark event (scheduled): ${mark.mark?.name}`);
          }
        }, delayMs);
        this.scheduledMarkTimers.push(id);
        // Marks don't advance time
        continue;
      }

      if (data.length === 0 || sampleRate <= 0) {
        continue;
      }

      // Create buffer and source
      const audioBuffer = this.audioContext.createBuffer(1, data.length, sampleRate);
      audioBuffer.getChannelData(0).set(data);

      const source = this.audioContext.createBufferSource();
      source.buffer = audioBuffer;

      if (this.analyser) {
        source.connect(this.analyser);
      }
      source.connect(this.audioContext.destination);

      // Cleanup on end and detect when finished
      source.onended = () => {
        // Remove from scheduled sources list
        this.scheduledSources = this.scheduledSources.filter((s) => s !== source);
        try { source.disconnect(); } catch (e) {}

        // Try to schedule more immediately if available
        this.scheduleFromQueue();

        // If nothing left scheduled and queue is empty, stop state
        if (this.scheduledSources.length === 0 && this.audioQueue.length === 0) {
          this.isPlaying = false;
          if (!this.isListening) {
            this.stopAudioStatsMonitoring();
          }
          if (this.onPlayStop) {
            this.onPlayStop();
          }
          if (this.scheduleTimer) {
            clearInterval(this.scheduleTimer);
            this.scheduleTimer = null;
          }
        }
      };

      // Schedule start
      const startAt = Math.max(this.audioContext.currentTime + 0.005, this.nextPlaybackTime);
      try {
        source.start(startAt);
      } catch (e) {
        // In case of invalid state, start immediately
        source.start();
      }
      this.scheduledSources.push(source);

      // Advance playback cursor by buffer duration (in audioContext time domain)
      const duration = audioBuffer.duration;
      this.nextPlaybackTime = startAt + duration;
      // Keep reference to current source for compatibility
      this.currentAudioSource = source;
    }
  }

  /**
   * Get the current audio level (volume) from the analyzer
   * This works for both microphone input and audio playback, depending on what"s currently active
   */
  public getAudioLevel(): number {
    if (!this.analyser) return 0;
    
    const dataArray = new Uint8Array(this.analyser.frequencyBinCount);
    this.analyser.getByteFrequencyData(dataArray);
    
    // Calculate average volume
    let sum = 0;
    for (let i = 0; i < dataArray.length; i++) {
      sum += dataArray[i];
    }
    
    return sum / dataArray.length / 255; // Normalize to 0-1 range
  }

  /**
   * Start monitoring audio levels for speech detection
   */
  private startAudioStatsMonitoring(): void {
    if (this.statsInterval) {
      clearInterval(this.statsInterval);
    }
    
    // Monitor audio levels at 100ms intervals
    this.statsInterval = window.setInterval(() => {
      const level = this.getAudioLevel();
      const isSpeaking = level > 0.1; // Simple threshold detection
      
      // Fire callback with audio stats
      if (this.onAudioStats) {
        this.onAudioStats({
          level,
          isSpeaking,
          isPlayback: this.isPlaying // Indicate if these stats are from playback
        });
      }
      
      // Optional speech detection logic could go here
      if (isSpeaking !== this.speechDetected) {
        this.speechDetected = isSpeaking;
        // Trigger speech events if needed
      }
    }, 100);
  }

  /**
   * Stop audio stats monitoring
   */
  private stopAudioStatsMonitoring(): void {
    if (this.statsInterval) {
      // Only stop monitoring if neither listening nor playing is active
      if (!this.isListening && !this.isPlaying) {
        clearInterval(this.statsInterval);
        this.statsInterval = null;
      }
    }
  }

  /**
   * Utility methods to check current state
   */
  public isCurrentlyConnected(): boolean {
    return this.isConnected;
  }

  public isCurrentlyListening(): boolean {
    return this.isListening;
  }

  public isCurrentlyPlaying(): boolean {
    return this.isPlaying;
  }
} 
