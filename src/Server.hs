module Server
    ( makeSocket
    , mainLoop
    ) where

import qualified Data.ByteString.Char8 as ByteString
import Network.Socket hiding (send, sendTo, recv, recvFrom)
import Network.Socket.ByteString
import Control.Concurrent

import Storage
import Query

makeSocket :: String -> IO Socket
makeSocket port = do
  let portNum = read port :: PortNumber
  sock <- socket AF_INET Stream 0
  setSocketOption sock ReuseAddr 1
  bind sock (SockAddrInet portNum iNADDR_ANY)
  listen sock 2
  return sock

mainLoop :: Socket -> Storage -> IO ()
mainLoop sock storage = do
  conn <- accept sock
  _ <- forkIO $ handleClient conn storage
  mainLoop sock storage

handleClient :: (Socket, SockAddr) -> Storage -> IO ()
handleClient (sock, addr) storage = do
  line <- recv sock maxRecv
  _ <- case parseQuery line of
    Nothing ->  sendError sock $ "Illegal query: " ++ ByteString.unpack line
    Just (q) -> do
      result <- query storage q
      sendResponse sock result
  handleClient (sock, addr) storage
  where
    maxRecv = 4096

    sendResponse :: Socket -> ByteString.ByteString -> IO ()
    sendResponse s resp = do
      _ <- send s $ ByteString.append resp $ ByteString.singleton '\n'
      return ()

    sendError :: Socket -> String -> IO ()
    sendError s err = do
      let packed = ByteString.pack $ "ERROR " ++ filter (/='\n') err
      sendResponse s packed

query :: Storage -> Query -> IO (ByteString.ByteString)
query storage (Get key) = do
  result <- get storage key
  case result of
    Just v -> return v
    Nothing -> return $ ByteString.pack "Not found"
query storage (Set key value) = do
  set storage key value
  return (ByteString.pack "OK")
query storage (Delete key) = do
  delete storage key
  return (ByteString.pack "OK")
query storage (Incr key) = do
  result <- increment storage key
  if result
    then return (ByteString.pack "OK")
    else return (ByteString.pack "Not found or not an int")
query storage (Decr key) = do
  result <- decrement storage key
  if result
    then return (ByteString.pack "OK")
    else return (ByteString.pack "Not found or not an int")
