module Server
    ( makeSocket
    , mainLoop
    , handleInput
    , query
    ) where

import           Data.Maybe (fromMaybe)
import           Data.ByteString.Short (ShortByteString)
import qualified Data.ByteString.Short as ShortBS
import           Data.ByteString.Char8 (ByteString)
import qualified Data.ByteString.Char8 as ByteString
import           Network.Socket hiding (send, sendTo, recv, recvFrom)
import           Network.Socket.ByteString
import           Control.Concurrent
import           Control.Monad
import           Control.Monad.State.Lazy

import           Query
import qualified Storage

bs :: String -> ByteString
bs = ByteString.pack

makeSocket :: String -> IO Socket
makeSocket port = do
  let portNum = read port :: PortNumber
  sock <- socket AF_INET Stream defaultProtocol
  setSocketOption sock ReuseAddr 1
  bind sock $ SockAddrInet portNum iNADDR_ANY
  listen sock maxQueue
  return sock
  where
    maxQueue = 1024

mainLoop :: Socket -> Storage.Storage -> IO ()
mainLoop sock storage = forever $ do
  putStrLn "Accepting.."
  conn <- accept sock
  putStrLn $ "New connection from " ++ (show $ snd conn)
  threadId <- forkIO $ evalStateT (handleClient storage conn) $ ShortBS.empty
  putStrLn $ "Forked new thread for " ++ (show $ snd conn) ++ " " ++ (show threadId)

handleClient :: Storage.Storage -> (Socket, SockAddr) -> StateT ShortByteString IO ()
handleClient storage conn@(sock, _)  = do
  input <- lift $ recv sock maxRecv
  if (ByteString.length input /= 0)
    then
    do
      fullInput <- state $
        fmap (ShortBS.toShort) .
        ByteString.breakEnd (=='\n') .
        (flip ByteString.append input) .
        ShortBS.fromShort
      lift $ do
        forM_ (ByteString.lines fullInput) (
          \line -> do
            response <- handleInput storage line
            sendAll sock $ ByteString.append response $ bs "\n"
          )
      handleClient storage conn
    else
    lift $ do
      threadId <- myThreadId
      putStrLn $ show threadId ++ " connection reset by peer"
      close sock
  where
    maxRecv = 1024


handleInput :: Storage.Storage -> ByteString -> IO ByteString
handleInput storage input = do
  case parseQuery input of
    Nothing -> return $ makeError $ "Illegal query: " ++ ByteString.unpack input
    Just (q) -> do
      result <- query storage q
      return $ result

query :: Storage.Storage -> Query -> IO (ByteString)
query storage (Get keys) = formatGet <$> Storage.get storage keys
query storage (Set key value) = do
  Storage.set storage key value
  return (bs "OK")
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

makeError :: String -> ByteString
makeError err = bs $ "ERROR " ++ filter (/='\n') err

checkResult :: String -> Bool -> IO (ByteString)
checkResult err value =
  if value
    then return $ bs "OK"
    else return $ makeError err

checkFound :: Maybe ByteString -> ByteString
checkFound = fromMaybe (bs "Not found")

formatGet :: [(Key, Value)] -> ByteString
formatGet [] = bs "Not found"
formatGet resp =
  ByteString.intercalate (bs "\n") $ map formatItem resp
  where
    formatItem :: (Key, Value) -> ByteString
    formatItem (key, value) =
      let keyStr = ByteString.append (bs "VALUE ") key
      in ByteString.intercalate (bs "\n") [keyStr, value, ByteString.pack "END"]
