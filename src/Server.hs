module Server
    ( makeSocket
    , mainLoop
    , handleInput
    , query
    ) where

import Data.Maybe (fromMaybe)
import qualified Data.ByteString.Char8 as ByteString
import Network.Socket hiding (send, sendTo, recv, recvFrom)
import Network.Socket.ByteString
import Control.Concurrent
import Control.Monad
import Control.Monad.State.Lazy

import Query
import qualified Storage

makeSocket :: String -> IO Socket
makeSocket port = withSocketsDo $ do
  let portNum = read port :: PortNumber
  sock <- socket AF_INET Stream defaultProtocol
  setSocketOption sock ReuseAddr 1
  bind sock $ SockAddrInet portNum iNADDR_ANY
  listen sock maxQueue
  return sock
  where
    maxQueue = 512

mainLoop :: Socket -> Storage.Storage -> IO ()
mainLoop sock storage = forever $ do
  putStrLn "Accepting.."
  conn <- accept sock
  putStrLn $ "New connection from " ++ (show $ snd conn)
  threadId <- forkIO $ evalStateT (handleClient storage conn) $ ByteString.pack ""
  putStrLn $ "Forked new thread for " ++ (show $ snd conn) ++ " " ++ (show threadId)

handleClient :: Storage.Storage ->  (Socket, SockAddr) -> StateT ByteString.ByteString IO ()
handleClient storage conn@(sock, _)  = do
  input <- lift $ recv sock maxRecv
  if (ByteString.length input /= 0)
    then
    do
      fullInput <- state $ ByteString.breakEnd (=='\n') . flip ByteString.append input
      lift $ do
        response <- handleInput storage fullInput
        sendAll sock $ response
      handleClient storage conn
    else
    lift $ do
      threadId <- myThreadId
      putStrLn $ show threadId ++ " connection reset by peer"
      close sock
  where
    maxRecv = 1024


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
query storage (Get keys) = formatGet <$> Storage.get storage keys
query storage (Set key value) = do
  Storage.set storage key value
  return (ByteString.pack "OK")
query storage (Delete key) = do
  Storage.delete storage key >>= checkResult "Not found"
query storage (Incr key amount) = checkFound <$> Storage.increment storage key amount
query storage (Decr key amount) = checkFound <$> Storage.decrement storage key amount
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

checkFound :: Maybe ByteString.ByteString -> ByteString.ByteString
checkFound = fromMaybe (ByteString.pack "Not found")

formatGet :: [(Key, Value)] -> ByteString.ByteString
formatGet [] = ByteString.pack "Not found"
formatGet resp =
  ByteString.intercalate (ByteString.pack "\n") $ map formatItem resp
  where
    formatItem :: (Key, Value) -> ByteString.ByteString
    formatItem (key, value) =
      let keyStr = ByteString.append (ByteString.pack "VALUE ") key
      in ByteString.intercalate (ByteString.pack "\n") [keyStr, value, ByteString.pack "END"]
