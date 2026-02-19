import React, { createContext, useContext, useState, useEffect, useRef, useCallback } from 'react';
import { WebSocketClient, WebSocketClientConfig, AudioStats, DebugMessage } from '../utils/WebSocketClient';

// Define the context shape
interface PrimVoicesContextType {
  connect: () => void;
  disconnect: () => void;
  startListening: () => Promise<void>;
  stopListening: () => void;
  sendTextEvent: (text: string) => void;
  isConnected: boolean;
  isListening: boolean;
  isPlaying: boolean;
  audioStats: AudioStats | null;
  debugMessages: DebugMessage[];
  error: string | null;
}

// Create the context with default values
const PrimVoicesContext = createContext<PrimVoicesContextType>({
  connect: async () => {},
  disconnect: async () => {},
  startListening: async () => {},
  stopListening: async () => {},
  sendTextEvent: async (_: string) => {},
  isConnected: false,
  isListening: false,
  isPlaying: false,
  audioStats: null,
  debugMessages: [],
  error: null,
});

// Hook for using the context
export const usePrimVoices = () => useContext(PrimVoicesContext);

interface PrimVoicesProviderProps {
  children: React.ReactNode;
  config: WebSocketClientConfig;
  autoConnect?: boolean;
}

export const PrimVoicesProvider: React.FC<PrimVoicesProviderProps> = ({
  children,
  config,
  autoConnect = false,
}) => {
  // Client reference
  const clientRef = useRef<WebSocketClient | null>(null);
  
  // State
  const [isConnected, setIsConnected] = useState(false);
  const [isListening, setIsListening] = useState(false);
  const [isPlaying, setIsPlaying] = useState(false);
  const [audioStats, setAudioStats] = useState<AudioStats | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [debugMessages, setDebugMessages] = useState<DebugMessage[]>([]);

  // Initialize client on mount
  useEffect(() => {
    try {
      clientRef.current = new WebSocketClient(config);
      
      // Setup callbacks
      clientRef.current.setCallbacks({
        onOpen: () => {
          setIsConnected(true);
          setError(null);
        },
        onClose: () => {
          setIsConnected(false);
        },
        onError: () => {
          setError('Connection error occurred');
        },
        onListeningStart: () => {
          setIsListening(true);
        },
        onListeningStop: () => {
          setIsListening(false);
        },
        onAudioStart: () => {
          setIsPlaying(true);
        },
        onAudioStop: () => {
          setIsPlaying(false);
        },
        onAudioStats: (stats) => {
          setAudioStats(stats);
        },
        onDebugMessage: (messages) => {
          setDebugMessages([...messages]);
        },
      });
      
      // Connect automatically if configured
      if (autoConnect) {
        clientRef.current.connect();
      }
      
      // Cleanup on unmount
      return () => {
        if (clientRef.current) {
          clientRef.current.disconnect();
          clientRef.current = null;
        }
      };
    } catch (err) {
      setError('Failed to initialize client');
      console.error('Error initializing PrimVoices client:', err);
    }
  }, [config, autoConnect]);

  // Connect to websocket
  const connect = async () => {
    if (clientRef.current) {
      try {
        await clientRef.current.connect();
      } catch (err: any) {
        const message = err?.message || 'Failed to connect';
        setError(message);
        throw err;
      }
    } else {
      setError('Client not initialized');
      throw new Error('Client not initialized');
    }
  };

  // Disconnect from websocket
  const disconnect = async () => {
    if (clientRef.current) {
      await clientRef.current.disconnect();
    }
  };

  // Start listening (recording from microphone)
  const startListening = async () => {
    if (clientRef.current) {
      try {
        await clientRef.current.startListening();
      } catch (err) {
        setError('Failed to start microphone');
      }
    } else {
      setError('Client not initialized');
    }
  };

  // Stop listening
  const stopListening = async () => {
    if (clientRef.current) {
      await clientRef.current.stopListening();
    }
  };

  // Send text message
  const sendTextEvent = async (text: string) => {
    if (clientRef.current && isConnected) {
      try {
        await clientRef.current.sendTextEvent(text);
      } catch (err) {
        setError('Failed to send message');
        console.error('Error sending text message:', err);
      }
    } else {
      setError('Client not connected');
    }
  };

  // The context value to provide
  const contextValue: PrimVoicesContextType = {
    connect,
    disconnect,
    startListening,
    stopListening,
    sendTextEvent,
    isConnected,
    isListening,
    isPlaying,
    audioStats,
    debugMessages,
    error,
  };

  return (
    <PrimVoicesContext.Provider value={contextValue}>
      {children}
    </PrimVoicesContext.Provider>
  );
};

export default PrimVoicesProvider; 
