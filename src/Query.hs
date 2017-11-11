module Query
  (
    parseQuery
  , Query(..)
  ) where

import qualified Data.ByteString.Char8 as ByteString

type Key = ByteString.ByteString
type Value = ByteString.ByteString

data Query = Get Key | Set Key Value deriving (Show)

parseQuery :: ByteString.ByteString -> Maybe Query
parseQuery qBStr =
  let qStr = ByteString.unpack qBStr
  in case words qStr of
    ("SET":key:value:[]) -> Just(Set (ByteString.pack key) (ByteString.pack value))
    ("GET":key:[]) -> Just(Get $ ByteString.pack key)
    _ -> Nothing
