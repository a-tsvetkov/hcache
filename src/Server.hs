module Server
    ( makeSocket
    , mainLoop
    ) where

import qualified Data.ByteString.Char8 as ByteString
import Network.Socket hiding (send, sendTo, recv, recvFrom)
import Network.Socket.ByteString
import Control.Concurrent
import Control.Monad
import Control.Monad.State

import Query
import qualified Storage

makeSocket :: String -> IO Socket
makeSocket port = do
  let portNum = read port :: PortNumber
  sock <- socket AF_INET Stream 0
  setSocketOption sock ReuseAddr 1
  bind sock (SockAddrInet portNum iNADDR_ANY)
  listen sock 2
  return sock

mainLoop :: Socket -> Storage.Storage -> IO ()
mainLoop sock storage = forever $ do
  conn <- accept sock
  _ <- forkIO $ evalStateT (handleClient conn storage) $ ByteString.pack ""
  return ()

handleClient :: (Socket, SockAddr) -> Storage.Storage -> StateT ByteString.ByteString IO ()
handleClient (sock, _) storage = forever $ do
  input <- lift $ recv sock maxRecv
  withUparsed <- state $ ByteString.breakEnd (=='\n') . (flip ByteString.append input)
  lift $ when (input /= ByteString.empty) $
    forM_ (ByteString.lines withUparsed) $
        \line -> do
          case parseQuery line of
            Nothing ->  sendError sock $ "Illegal query: " ++ ByteString.unpack line
            Just (q) -> do
              result <-  query storage q
              sendResponse sock result
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

query :: Storage.Storage -> Query -> IO (ByteString.ByteString)
query storage (Get keys) = do
  result <- Storage.get storage keys
  case result of
    [] -> return $ ByteString.pack "Not found"
    v -> return (formatResponse v)
query storage (Set key value) = do
  Storage.set storage key value
  return (ByteString.pack "OK")
query storage (Delete key) = do
  Storage.delete storage key
  return (ByteString.pack "OK")
query storage (Incr key amount) =
  Storage.increment storage key amount >>= checkResult "OK" "Not found or not an int"
query storage (Decr key amount) =
  Storage.decrement storage key amount >>= checkResult "OK" "Not found or not an int"
query storage (Add key value) =
  Storage.add storage key value >>= checkResult "OK" "Already exists"
query storage (Replace key value) =
  Storage.replace storage key value >>= checkResult "OK" "Not found"
query storage (Append key value) =
  Storage.append storage key value >>= checkResult "OK" "Not found"
query storage (Prepend key value) =
  Storage.prepend storage key value >>= checkResult "OK" "Not found"

checkResult :: String -> String -> Bool -> IO (ByteString.ByteString)
checkResult ok err value =
  if value
    then return (ByteString.pack ok)
    else return (ByteString.pack err)

formatResponse :: [(Key, Value)] -> ByteString.ByteString
formatResponse resp =
  ByteString.intercalate (ByteString.pack "\n") $ map formatItem resp
  where
    formatItem :: (Key, Value) -> ByteString.ByteString
    formatItem (key, value) =
      let keyStr = ByteString.append (ByteString.pack "VALUE ") key
      in ByteString.intercalate (ByteString.pack "\n") [keyStr, value, ByteString.pack "END"]
