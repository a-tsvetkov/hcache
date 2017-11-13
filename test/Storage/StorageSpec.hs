module Storage.StorageSpec
  (
    main
  , spec
  ) where

import Test.Hspec
import Control.Monad.STM
import qualified Data.ByteString.Char8 as B
import qualified STMContainers.Map as Map
import Storage

main :: IO ()
main = hspec spec

spec :: Spec
spec = do
  describe "storage" $ do
    context "get" $ do
      it "should fetch single value as a singleton list" $ do
        storage <- initStorage
        set storage (B.pack "key") (B.pack "value")
        value <- get storage [B.pack "key"]
        value `shouldBe` [(B.pack "key", B.pack "value")]

      it "should fetch multiple values as a list" $ do
        storage <- initStorage
        set storage (B.pack "key1") (B.pack "value1")
        set storage (B.pack "key2") (B.pack "value2")
        set storage (B.pack "key3") (B.pack "value3")
        value <- get storage [B.pack "key1", B.pack "key2", B.pack "key3"]
        value `shouldBe` [(B.pack "key1", B.pack "value1"), (B.pack "key2", B.pack "value2"), (B.pack "key3", B.pack "value3")]

      it "should return empty list if no values found" $ do
        storage <- initStorage
        value <- get storage [B.pack "key1", B.pack "key2", B.pack "key3"]
        value `shouldBe` []

    context "set" $ do
      it "should save value to the storage" $ do
        storage <- initStorage
        set storage (B.pack "key") (B.pack "value")
        value <- atomically $ Map.lookup (B.pack "key") storage
        value `shouldBe` Just (B.pack "value")

      it "should overwrite existing value" $ do
        storage <- initStorage
        set storage (B.pack "key") (B.pack "value")
        set storage (B.pack "key") (B.pack "new value")
        value <- atomically $ Map.lookup (B.pack "key") storage
        value `shouldBe` Just (B.pack "new value")

    context "add" $ do
      it "should create new value" $ do
        storage <- initStorage
        ret <- add storage (B.pack "key") (B.pack "value")
        value <- atomically $ Map.lookup (B.pack "key") storage
        ret `shouldBe` True
        value `shouldBe` Just (B.pack "value")

      it "should not overwrite existing value" $ do
        storage <- initStorage
        set storage (B.pack "key") (B.pack "value")
        ret <- add storage (B.pack "key") (B.pack "new value")
        value <- atomically $ Map.lookup (B.pack "key") storage
        ret `shouldBe` False
        value `shouldBe` Just (B.pack "value")

    context "replace" $ do
      it "should not create new value" $ do
        storage <- initStorage
        ret <- replace storage (B.pack "key") (B.pack "value")
        value <- atomically $ Map.lookup (B.pack "key") storage
        ret `shouldBe` False
        value `shouldBe` Nothing

      it "should overwrite existing value" $ do
        storage <- initStorage
        set storage (B.pack "key") (B.pack "value")
        ret <- replace storage (B.pack "key") (B.pack "new value")
        value <- atomically $ Map.lookup (B.pack "key") storage
        ret `shouldBe` True
        value `shouldBe` Just (B.pack "new value")

    context "delete" $ do
      it "should delete existing value" $ do
        storage <- initStorage
        let key = (B.pack "key")
        set storage key (B.pack "value")
        result <- delete storage key
        value <- atomically $ Map.lookup key storage
        value `shouldBe` Nothing
        result `shouldBe` True

      it "should return False if value does not exist" $ do
        storage <- initStorage
        let key = (B.pack "key")
        result <- delete storage key
        value <- atomically $ Map.lookup key storage
        value `shouldBe` Nothing
        result `shouldBe` False

      it "should not affect other values" $ do
        storage <- initStorage
        let key = (B.pack "key")
            key1 = (B.pack "key1")
        set storage key (B.pack "value")
        set storage key1 (B.pack "value")
        result <- delete storage key
        value <- atomically $ Map.lookup key1 storage
        value `shouldBe` Just (B.pack "value")
        result `shouldBe` True

    context "increment" $ do
      it "should add supplied value to existing one" $ do
        storage <- initStorage
        let key = (B.pack "key")
        set storage key (B.pack "10")
        res <- increment storage key 5
        value <- atomically $ Map.lookup key storage
        res `shouldBe` True
        value `shouldBe` Just (B.pack "15")

      it "should fail if value does not exist" $ do
        storage <- initStorage
        let key = (B.pack "key")
        res <- increment storage key 5
        value <- atomically $ Map.lookup key storage
        res `shouldBe` False
        value `shouldBe` Nothing

      it "should properly handle big numbers" $ do
        storage <- initStorage
        let key = (B.pack "key")
        set storage key (B.pack "1922337203685477580710")
        res <- increment storage key 5
        value <- atomically $ Map.lookup key storage
        res `shouldBe` True
        value `shouldBe` Just (B.pack "1922337203685477580715")

      it "should properly handle negative numbers" $ do
        storage <- initStorage
        let key = (B.pack "key")
        set storage key (B.pack "-100")
        res <- increment storage key 5
        value <- atomically $ Map.lookup key storage
        res `shouldBe` True
        value `shouldBe` Just (B.pack "-95")

      it "should accept big number as an argument" $ do
        storage <- initStorage
        let key = (B.pack "key")
        set storage key (B.pack "100")
        res <- increment storage key 1922337203685477580715
        value <- atomically $ Map.lookup key storage
        res `shouldBe` True
        value `shouldBe` Just (B.pack "1922337203685477580815")

    context "decrement" $ do
      it "should substract supplied value to existing one" $ do
        storage <- initStorage
        let key = (B.pack "key")
        set storage key (B.pack "10")
        res <- decrement storage key 5
        value <- atomically $ Map.lookup key storage
        res `shouldBe` True
        value `shouldBe` Just (B.pack "5")

      it "should fail if value does not exist" $ do
        storage <- initStorage
        let key = (B.pack "key")
        res <- decrement storage key 5
        value <- atomically $ Map.lookup key storage
        res `shouldBe` False
        value `shouldBe` Nothing

      it "should properly handle big numbers" $ do
        storage <- initStorage
        let key = (B.pack "key")
        set storage key (B.pack "1922337203685477580810")
        res <- decrement storage key 5
        value <- atomically $ Map.lookup key storage
        res `shouldBe` True
        value `shouldBe` Just (B.pack "1922337203685477580805")

      it "should properly handle negative numbers" $ do
        storage <- initStorage
        let key = (B.pack "key")
        set storage key (B.pack "-100")
        res <- decrement storage key 5
        value <- atomically $ Map.lookup key storage
        res `shouldBe` True
        value `shouldBe` Just (B.pack "-105")

      it "should accept big number as an argument" $ do
        storage <- initStorage
        let key = (B.pack "key")
        set storage key (B.pack "100")
        res <- decrement storage key 1922337203685477580715
        value <- atomically $ Map.lookup key storage
        res `shouldBe` True
        value `shouldBe` Just (B.pack "-1922337203685477580615")

    context "append" $ do
      it "should append a string to the end of the value" $ do
        storage <- initStorage
        let key = (B.pack "key")
        set storage key (B.pack "value")
        res <- append storage key (B.pack " appended")
        value <- atomically $ Map.lookup key storage
        res `shouldBe` True
        value `shouldBe` Just (B.pack "value appended")

      it "should handle empty string properly" $ do
        storage <- initStorage
        let key = (B.pack "key")
        set storage key (B.pack "value")
        res <- append storage key (B.pack "")
        value <- atomically $ Map.lookup key storage
        res `shouldBe` True
        value `shouldBe` Just (B.pack "value")

      it "should fail if value does not exist" $ do
        storage <- initStorage
        let key = (B.pack "key")
        res <- append storage key (B.pack " appended")
        value <- atomically $ Map.lookup key storage
        res `shouldBe` False
        value `shouldBe` Nothing

    context "prepend" $ do
      it "should prepend a string to the end of the value" $ do
        storage <- initStorage
        let key = (B.pack "key")
        set storage key (B.pack "value")
        res <- prepend storage key (B.pack "prepended ")
        value <- atomically $ Map.lookup key storage
        res `shouldBe` True
        value `shouldBe` Just (B.pack "prepended value")

      it "should handle empty string properly" $ do
        storage <- initStorage
        let key = (B.pack "key")
        set storage key (B.pack "value")
        res <- prepend storage key (B.pack "")
        value <- atomically $ Map.lookup key storage
        res `shouldBe` True
        value `shouldBe` Just (B.pack "value")

      it "should fail if value does not exist" $ do
        storage <- initStorage
        let key = (B.pack "key")
        res <- prepend storage key (B.pack "prepended ")
        value <- atomically $ Map.lookup key storage
        res `shouldBe` False
        value `shouldBe` Nothing
