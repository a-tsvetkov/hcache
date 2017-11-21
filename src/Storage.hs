module Storage
  (
    initStorage
  , Storage
  , get
  , getOne
  , set
  , delete
  , increment
  , decrement
  , add
  , replace
  , append
  , prepend
  ) where

import qualified Focus as Focus
import           Data.Maybe (catMaybes)
import qualified Data.ByteString.Char8 as ByteString
import           Control.Monad
import qualified Data.HashTable.IO.ConcurrentLinear as Map

import Query
import Serialization

type Storage = Map.HashTable Key Value

initStorage :: IO Storage
initStorage = Map.new

getOne :: Storage -> Key -> IO (Maybe Value)
getOne storage key = Map.lookup storage key

get :: Storage -> [Key] -> IO [(Key, Value)]
get storage keys = catMaybes <$> forM keys (
  \key -> do
    result <- getOne storage key
    return (result >>= (\v -> return (key, v)))
  )

set :: Storage -> Key -> Value -> IO ()
set storage key value = Map.insert storage key value

add :: Storage -> Key -> Value -> IO Bool
add storage key value = Map.focus storage key (
  \v ->
    case v of
      Just _ -> (False, Focus.Keep)
      Nothing -> do
        (True, Focus.Replace value)
  )

replace :: Storage -> Key -> Value -> IO Bool
replace storage key value = Map.focus storage key (
  \v ->
    case v of
      Just _ -> do
        (True, Focus.Replace value)
      Nothing -> (False, Focus.Keep)
  )

delete :: Storage -> Key -> IO Bool
delete storage key = Map.focus storage key (
    \value ->
      case value of
        Just _ -> (True, Focus.Remove)
        Nothing -> (False, Focus.Keep)
    )

increment :: Storage -> Key -> Integer -> IO (Maybe ByteString.ByteString)
increment storage key amount = updateInteger storage key (+amount)

decrement :: Storage -> Key -> Integer -> IO (Maybe ByteString.ByteString)
decrement storage key amount = updateInteger storage key (subtract amount)

append :: Storage -> Key -> Value -> IO Bool
append storage key value = updateValue storage key $ flip (ByteString.append) value

prepend :: Storage -> Key -> Value -> IO Bool
prepend storage key value = updateValue storage key $ ByteString.append value

updateValue :: Storage -> Key -> (Value -> Value) -> IO Bool
updateValue storage key f = Map.focus storage key (
  \value ->
    case value of
      Nothing -> (False, Focus.Keep)
      Just v -> (True, Focus.Replace $ f v)
    )

updateInteger :: Storage -> Key -> (Integer -> Integer) -> IO (Maybe ByteString.ByteString)
updateInteger storage key f = Map.focus storage key (
  \value ->
    case value >>= readInteger of
      Nothing -> (Nothing, Focus.Keep)
      Just int -> let newValue = writeInteger $ f int
        in (Just newValue, Focus.Replace $ newValue)
  )
