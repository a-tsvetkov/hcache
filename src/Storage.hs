module Storage
  (
    initStorage
  , Storage
  , get
  , set
  , delete
  , increment
  , decrement
  ) where

import Query
import Control.Monad.STM
import Data.ByteString.Builder
import Data.ByteString.Lazy (toStrict)
import qualified Data.ByteString.Char8 as ByteString
import qualified STMContainers.Map as Map

type Storage = Map.Map Key Value

initStorage :: IO Storage
initStorage = Map.newIO

get :: Storage -> Key -> IO (Maybe Value)
get storage key = atomically $ Map.lookup key storage

set :: Storage -> Key -> Value -> IO ()
set storage key value = atomically $ Map.insert value key storage

delete :: Storage -> Key -> IO ()
delete storage key = atomically $ Map.delete key storage

increment :: Storage -> Key -> IO Bool
increment storage key = atomically $ updateInteger storage key (+1)

decrement :: Storage -> Key -> IO Bool
decrement storage key = atomically $ updateInteger storage key (subtract 1)

updateInteger :: Storage -> Key -> (Integer -> Integer) -> STM Bool
updateInteger storage key f = do
    value <- Map.lookup key storage
    case value >>= readInteger of
      Nothing -> return False
      Just int -> do
        Map.insert (writeInteger $ f int) key storage
        return True

readInteger :: ByteString.ByteString -> Maybe Integer
readInteger s = fst <$> ByteString.readInteger s

writeInteger :: Integer -> ByteString.ByteString
writeInteger int = toStrict $ toLazyByteString $ integerDec int
