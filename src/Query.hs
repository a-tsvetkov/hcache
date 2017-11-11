module Query
  (
    parseQuery
  , Key
  , Value
  , Query(..)
  ) where

import Data.Char (toUpper)
import qualified Data.ByteString.Char8 as ByteString

type Key = ByteString.ByteString
type Value = ByteString.ByteString

data Query = Get Key | Set Key Value | Delete Key deriving (Show)

parseQuery :: ByteString.ByteString -> Maybe Query
parseQuery qBStr = do
  splitted <- splitCommand qBStr
  case splitted of
    ("SET", (key:value:[])) -> return (Set key value)
    ("GET", (key:[])) -> return (Get key)
    ("DELETE", (key:[])) -> return (Delete key)
    _ -> Nothing
  where
    splitCommand :: ByteString.ByteString -> Maybe (String, [ByteString.ByteString])
    splitCommand str =
      case ByteString.words str of
        (cmd:args) -> let command = map toUpper $ ByteString.unpack cmd in Just (command, args)
        _ -> Nothing
