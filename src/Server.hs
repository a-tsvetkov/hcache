module Server
    ( makeSocket
    , mainLoop
    , handleInput
    , query
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
  threadId <- forkIO $ evalStateT (handleClient conn storage) $ ByteString.pack ""
  putStrLn $ "New connection from " ++ (show $ snd conn) ++ " " ++ (show threadId)

  return ()

handleClient :: (Socket, SockAddr) -> Storage.Storage -> StateT ByteString.ByteString IO ()
handleClient conn@(sock, _) storage = do
  input <- lift $ recv sock maxRecv
  if (ByteString.length input /= 0)
    then
    do
      fullInput <- state $ ByteString.breakEnd (=='\n') . flip ByteString.append input
      response <- lift $ handleInput storage fullInput
      lift $ sendAll sock $ response
      handleClient conn storage
    else
    do
      threadId <- lift $ myThreadId
      lift $ putStrLn $ show threadId ++ " connection reset by peer"
      lift $ close sock
      return ()
  where
    maxRecv = 4096


handleInput :: Storage.Storage -> ByteString.ByteString -> IO ByteString.ByteString
handleInput storage input = do
  results <- forM (ByteString.lines input) $
    \line -> do
      case parseQuery line of
        Nothing -> return $ makeError $ "Illegal query: " ++ ByteString.unpack line
        Just (q) -> do
          result <- query storage q
          return $ result

  return $ ByteString.unlines results

query :: Storage.Storage -> Query -> IO (ByteString.ByteString)
query storage (Get keys) = do
  result <- Storage.get storage keys
  case result of
    [] -> return $ ByteString.pack "Not found"
    v -> return (formatData v)
query storage (Set key value) = do
  Storage.set storage key value
  return (ByteString.pack "OK")
query storage (Delete key) = do
  Storage.delete storage key >>= checkResult "Not found"
query storage (Incr key amount) =
  Storage.increment storage key amount >>= checkResult "Not found"
query storage (Decr key amount) =
  Storage.decrement storage key amount >>= checkResult "Not found"
query storage (Add key value) =
  Storage.add storage key value >>= checkResult "Already exists"
query storage (Replace key value) =
  Storage.replace storage key value >>= checkResult "Not found"
query storage (Append key value) =
  Storage.append storage key value >>= checkResult "Not found"
query storage (Prepend key value) =
  Storage.prepend storage key value >>= checkResult "Not found"

makeError :: String -> ByteString.ByteString
makeError err = ByteString.pack $ "ERROR " ++ filter (/='\n') err


checkResult :: String -> Bool -> IO (ByteString.ByteString)
checkResult err value =
  if value
    then return $ ByteString.pack "OK"
    else return $ makeError err

formatData :: [(Key, Value)] -> ByteString.ByteString
formatData resp =
  ByteString.intercalate (ByteString.pack "\n") $ map formatItem resp
  where
    formatItem :: (Key, Value) -> ByteString.ByteString
    formatItem (key, value) =
      let keyStr = ByteString.append (ByteString.pack "VALUE ") key
      in ByteString.intercalate (ByteString.pack "\n") [keyStr, value, ByteString.pack "END"]
