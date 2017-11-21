module Server.ServerSpec
  (
    main
  , spec
  ) where

import Test.Hspec
import qualified Data.ByteString.Char8 as B
import Server
import Storage

main :: IO ()
main = hspec spec

spec :: Spec
spec = do
  describe "server" $ do
    context "handleInput" $ do
      it "should get value" $ do
        storage <- initStorage
        set storage (B.pack "key") (B.pack "value")
        resp <- handleInput storage $ B.pack "get key\n"
        resp `shouldBe` B.pack "VALUE key\nvalue\nEND\n"

      it "should get multiple values" $ do
        storage <- initStorage
        set storage (B.pack "key1") (B.pack "value1")
        set storage (B.pack "key2") (B.pack "value2")
        resp <- handleInput storage $ B.pack "get key1 key2\n"
        resp `shouldBe` B.pack "VALUE key1\nvalue1\nEND\nVALUE key2\nvalue2\nEND\n"

      it "should return Not found on non existing value" $ do
        storage <- initStorage
        resp <- handleInput storage $ B.pack "get key1\n"
        resp `shouldBe` B.pack "Not found\n"

      it "should return Not found on multiple non existing values" $ do
        storage <- initStorage
        resp <- handleInput storage $ B.pack "get key1 key2\n"
        resp `shouldBe` B.pack "Not found\n"

      it "should output only found values if multiple values requested but not all are found" $ do
        storage <- initStorage
        set storage (B.pack "key1") (B.pack "value1")
        resp <- handleInput storage $ B.pack "get key1 key2\n"
        resp `shouldBe` B.pack "VALUE key1\nvalue1\nEND\n"

      it "should set value" $ do
        storage <- initStorage
        resp <- handleInput storage $ B.pack "set key value\n"
        value <- getOne storage (B.pack "key")
        resp `shouldBe` B.pack "OK\n"
        value `shouldBe` Just (B.pack "value")

      it "should delete value" $ do
        storage <- initStorage
        set storage (B.pack "key") (B.pack "value")
        resp <- handleInput storage $ B.pack "delete key\n"
        value <- getOne storage (B.pack "key")
        resp `shouldBe` B.pack "OK\n"
        value `shouldBe` Nothing

      it "should return Not found when trying to delete non existent value" $ do
        storage <- initStorage
        resp <- handleInput storage $ B.pack "delete key\n"
        resp `shouldBe` B.pack "ERROR Not found\n"

      it "should increment and return value" $ do
        storage <- initStorage
        set storage (B.pack "key") (B.pack "100")
        resp <- handleInput storage $ B.pack "incr key 5\n"
        value <- getOne storage (B.pack "key")
        resp `shouldBe` (B.pack "105\n")
        value `shouldBe` Just (B.pack "105")

      it "should return error when trying to increment by string" $ do
        storage <- initStorage
        set storage (B.pack "key") (B.pack "100")
        resp <- handleInput storage $ B.pack "incr key test\n"
        value <- getOne storage (B.pack "key")
        resp `shouldBe` B.pack "ERROR Illegal query: incr key test\n"
        value `shouldBe` Just (B.pack "100")

      it "should decrement and return value" $ do
        storage <- initStorage
        set storage (B.pack "key") (B.pack "100")
        resp <- handleInput storage $ B.pack "decr key 5\n"
        value <- getOne storage (B.pack "key")
        resp `shouldBe` (B.pack "95\n")
        value `shouldBe` Just (B.pack "95")

      it "should return error when trying to decrement by string" $ do
        storage <- initStorage
        set storage (B.pack "key") (B.pack "100")
        resp <- handleInput storage $ B.pack "decr key test\n"
        value <- getOne storage (B.pack "key")
        resp `shouldBe` B.pack "ERROR Illegal query: decr key test\n"
        value `shouldBe` Just (B.pack "100")

      it "should add value" $ do
        storage <- initStorage
        resp <- handleInput storage $ B.pack "add key value\n"
        value <- getOne storage  (B.pack "key")
        resp `shouldBe` B.pack "OK\n"
        value `shouldBe` Just (B.pack "value")

      it "should return error when trying to add an existing value" $ do
        storage <- initStorage
        set storage (B.pack "key") (B.pack "value")
        resp <- handleInput storage $ B.pack "add key newvalue\n"
        value <- getOne storage  (B.pack "key")
        resp `shouldBe` B.pack "ERROR Already exists\n"
        value `shouldBe` Just (B.pack "value")

      it "should replace value" $ do
        storage <- initStorage
        set storage (B.pack "key") (B.pack "value")
        resp <- handleInput storage $ B.pack "replace key newvalue\n"
        value <- getOne storage  (B.pack "key")
        resp `shouldBe` B.pack "OK\n"
        value `shouldBe` Just (B.pack "newvalue")

      it "should return error when trying to replace non existing value" $ do
        storage <- initStorage
        resp <- handleInput storage $ B.pack "replace key newvalue\n"
        value <- getOne storage  (B.pack "key")
        resp `shouldBe` B.pack "ERROR Not found\n"
        value `shouldBe` Nothing

      it "should append string" $ do
        storage <- initStorage
        set storage (B.pack "key") (B.pack "value")
        resp <- handleInput storage $ B.pack "append key _appended\n"
        value <- getOne storage  (B.pack "key")
        resp `shouldBe` B.pack "OK\n"
        value `shouldBe` Just (B.pack "value_appended")

      it "should return error when trying to append to non existentvalue" $ do
        storage <- initStorage
        resp <- handleInput storage $ B.pack "append key _appended\n"
        value <- getOne storage  (B.pack "key")
        resp `shouldBe` B.pack "ERROR Not found\n"
        value `shouldBe` Nothing

      it "should prepend string" $ do
        storage <- initStorage
        set storage (B.pack "key") (B.pack "value")
        resp <- handleInput storage $ B.pack "prepend key prepended_\n"
        value <- getOne storage  (B.pack "key")
        resp `shouldBe` B.pack "OK\n"
        value `shouldBe` Just (B.pack "prepended_value")

      it "should return error when trying to prepend to non existentvalue" $ do
        storage <- initStorage
        resp <- handleInput storage $ B.pack "prepend key prepended_\n"
        value <- getOne storage  (B.pack "key")
        resp `shouldBe` B.pack "ERROR Not found\n"
        value `shouldBe` Nothing
