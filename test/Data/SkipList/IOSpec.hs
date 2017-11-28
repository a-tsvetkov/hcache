module Data.SkipList.IOSpec
  (
    main
  , spec
  ) where

import           Test.Hspec
import           Data.List
import           Control.Monad
import qualified Data.SkipList.IO as SL
import           Focus (Decision(..))

main :: IO ()
main = hspec spec

spec :: Spec
spec = do
  describe "Data.SkipList.IO" $ do
    context "lookup" $ do
      it "should get value by key" $ do
        sl <- SL.new
        SL.insert sl "foo" "bar"
        res <- SL.lookup sl "foo"
        res `shouldBe` Just "bar"

      it "should get value when multiple values exist" $ do
        sl <- SL.new
        forM_ ['a'..'z'] (\c -> SL.insert sl ("key" ++ [c])  ("value" ++ [c]))
        res <- SL.lookup sl "keyp"
        res `shouldBe` Just "valuep"

      it "should get smallest value of multiple" $ do
        sl <- SL.new
        forM_ ['a'..'z'] (\c -> SL.insert sl ("key" ++ [c])  ("value" ++ [c]))
        res <- SL.lookup sl "keya"
        res `shouldBe` Just "valuea"

      it "should get biggest value of multiple" $ do
        sl <- SL.new
        forM_ ['a'..'z'] (\c -> SL.insert sl ("key" ++ [c])  ("value" ++ [c]))
        res <- SL.lookup sl "keyz"
        res `shouldBe` Just "valuez"

    context "insert" $ do
      it "should create new value" $ do
        sl <- SL.new
        SL.insert sl "foo" "bar"
        res <- SL.lookup sl "foo"
        res `shouldBe` Just "bar"

      it "should overwrite existing value" $ do
        sl <- SL.new
        SL.insert sl "foo" "bar"
        SL.insert sl "foo" "barbarbar"
        res <- SL.lookup sl "foo"
        res `shouldBe` Just "barbarbar"

      it "should insert value when multiple values exist" $ do
        sl <- SL.new
        forM_ ['a'..'z'] (\c -> SL.insert sl ("key" ++ [c])  ("value" ++ [c]))
        SL.insert sl "foo" "bar"
        res <- SL.lookup sl "foo"
        res `shouldBe` Just "bar"

      it "should overwrite value when multiple values exist" $ do
        sl <- SL.new
        forM_ ['a'..'z'] (\c -> SL.insert sl ("key" ++ [c])  ("value" ++ [c]))
        SL.insert sl "keym" "bar"
        res <- SL.lookup sl "keym"
        res `shouldBe` Just "bar"

      it "should insert smallest value" $ do
        sl <- SL.new
        forM_ ['b'..'z'] (\c -> SL.insert sl ("key" ++ [c])  ("value" ++ [c]))
        SL.insert sl "keya" "bar"
        res <- SL.lookup sl "keya"
        res `shouldBe` Just "bar"

      it "should insert biggest value" $ do
        sl <- SL.new
        forM_ ['b'..'y'] (\c -> SL.insert sl ("key" ++ [c])  ("value" ++ [c]))
        SL.insert sl "keyz" "bar"
        res <- SL.lookup sl "keyz"
        res `shouldBe` Just "bar"

      it "should preserve existing values" $ do
        let assocs = map (\c -> ("key" ++ [c], "value" ++ [c])) $ take 32 ['0'..'z']
        sl <- SL.new
        forM_ assocs $ uncurry (SL.insert sl)
        SL.insert sl "foo" "bar"
        forM_ assocs (
          \(k, v) -> do
            val <- SL.lookup sl k
            val `shouldBe` Just v
          )

    context "delete" $ do
      it "should delete value" $ do
        sl <- SL.new
        SL.insert sl "foo" "bar"
        SL.delete sl "foo"
        res <- SL.lookup sl "foo"
        res `shouldBe` Nothing

      it "should not interact with other values" $ do
        let assocs = map (\c -> ("key" ++ [c], "value" ++ [c])) $ take 32 ['0'..'z']
        sl <- SL.new
        forM_ assocs $ uncurry $ SL.insert sl
        SL.delete sl "keyk"
        res <- SL.lookup sl "keyk"
        res `shouldBe` Nothing
        forM_ (filter ((/="keyk") . fst) assocs) (
          \(k, v) -> do
            val <- SL.lookup sl k
            val `shouldBe` Just v
          )

      it "should delete smallest value" $ do
        let assocs = map (\c -> ("key" ++ [c], "value" ++ [c])) $ take 32 ['0'..'z']
        sl <- SL.new
        forM_ assocs $ uncurry $ SL.insert sl
        SL.delete sl "keya"
        res <- SL.lookup sl "keya"
        res `shouldBe` Nothing

      it "should delete biggest" $ do
        let assocs = map (\c -> ("key" ++ [c], "value" ++ [c])) $ take 32 ['0'..'z']
        sl <- SL.new
        forM_ assocs $ uncurry $ SL.insert sl
        SL.delete sl "keyz"
        res <- SL.lookup sl "keya"
        res `shouldBe` Nothing
