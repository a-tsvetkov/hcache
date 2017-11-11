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
    Nothing ->  send sock $ ByteString.append (ByteString.pack "Illegal query: ") line
    Just (q) -> do
      result <- query storage q
      send sock $ ByteString.append result $ ByteString.singleton '\n'
  handleClient (sock, addr) storage
  where
    maxRecv = 4096

query :: Storage -> Query -> IO (ByteString.ByteString)
query storage (Get key) = do
  result <- get storage key
  case result of
    Just v -> return v
    Nothing -> return $ ByteString.pack "Not found"
query storage (Set key value) = do
  set storage key value
  return (ByteString.pack "OK")
