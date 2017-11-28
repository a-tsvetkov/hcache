module Data.SkipList.IOSpec
  (
    main
  , spec
  ) where

import           Test.Hspec
import           Data.List (partition)
import           Control.Monad
import qualified Control.Monad.Parallel as P
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
        let assocs = map (\c -> ("key" ++ [c], "value" ++ [c])) ['0'..'z']
        sl <- SL.new
        forM_ assocs $ uncurry (SL.insert sl)
        SL.insert sl "foo" "bar"
        forM_ assocs (
          \(k, v) -> do
            val <- SL.lookup sl k
            val `shouldBe` Just v
          )

    context "parallel insert" $ do
      it "should not lose values" $ do
        let assocs = map (\c -> ("key" ++ [c], "value" ++ [c])) ['0'..'z']
        sl <- SL.new
        P.forM_ assocs $ uncurry (SL.insert sl)
        forM_ assocs (
          \(k, v) -> do
            val <- SL.lookup sl k
            val `shouldBe` Just v
          )

      it "should preserve existing values" $ do
        let assocs1 = map (\c -> ("key" ++ [c], "value" ++ [c])) ['0'..'z']
            assocs2 = map (\c -> ("k" ++ [c], "v" ++ [c])) ['0'..'z']
        sl <- SL.new
        forM_ assocs1 $ uncurry (SL.insert sl)
        P.forM_ assocs2 $ uncurry (SL.insert sl)
        forM_ assocs1 (
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
        let assocs = map (\c -> ("key" ++ [c], "value" ++ [c])) ['0'..'z']
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

      it "should not interact with other values if in between" $ do
        let assocs = map (\c -> ("key" ++ [c], "value" ++ [c])) $ ['0'..'z']
            (d, r) = partition (odd . fst) $ zip [0..] assocs
            delete = map snd d
            remain = map snd r
        sl <- SL.new
        forM_ assocs $ uncurry (SL.insert sl)
        forM_ delete $ SL.delete sl . fst
        forM_ remain (
          \(k, v) -> do
            val <- SL.lookup sl k
            val `shouldBe` Just v
          )

      it "should do nothing if value does not exist" $ do
        let assocs = map (\c -> ("key" ++ [c], "value" ++ [c])) ['0'..'z']
        sl <- SL.new
        forM_ assocs $ uncurry $ SL.insert sl
        SL.delete sl "foo"
        res <- SL.lookup sl "foo"
        res `shouldBe` Nothing
        forM_ assocs (
          \(k, v) -> do
            val <- SL.lookup sl k
            val `shouldBe` Just v
          )

      it "should delete smallest value" $ do
        let assocs = map (\c -> ("key" ++ [c], "value" ++ [c])) ['0'..'z']
        sl <- SL.new
        forM_ assocs $ uncurry $ SL.insert sl
        SL.delete sl "keya"
        res <- SL.lookup sl "keya"
        res `shouldBe` Nothing

      it "should delete biggest" $ do
        let assocs = map (\c -> ("key" ++ [c], "value" ++ [c])) ['0'..'z']
        sl <- SL.new
        forM_ assocs $ uncurry $ SL.insert sl
        SL.delete sl "keyz"
        res <- SL.lookup sl "keyz"
        res `shouldBe` Nothing

    context "parallel delete" $ do
      it "sould delete all the values" $ do
        let assocs = map (\c -> ("key" ++ [c], "value" ++ [c])) ['0'..'z']
        sl <- SL.new
        forM_ assocs $ uncurry $ SL.insert sl
        P.forM_ assocs $ SL.delete sl . fst
        forM_ assocs (
          \(k, _) -> do
            val <- SL.lookup sl k
            val `shouldBe` Nothing
          )

      it "should not interact with other values aside" $ do
        let assocs = map (\c -> ("key" ++ [c], "value" ++ [c])) $ ['0'..'z']
            (delete, remain) = splitAt 32 assocs
        sl <- SL.new
        forM_ assocs $ uncurry (SL.insert sl)
        P.forM_ delete $ SL.delete sl . fst
        forM_ remain (
          \(k, v) -> do
            val <- SL.lookup sl k
            val `shouldBe` Just v
          )

      it "should not interact with other values if in between" $ do
        let assocs = map (\c -> ("key" ++ [c], "value" ++ [c])) $ ['0'..'z']
            (d, r) = partition (odd . fst) $ zip [0..] assocs
            delete = map snd d
            remain = map snd r
        sl <- SL.new
        forM_ assocs $ uncurry (SL.insert sl)
        P.forM_ delete $ SL.delete sl . fst
        forM_ remain (
          \(k, v) -> do
            val <- SL.lookup sl k
            val `shouldBe` Just v
          )

    context "focus" $ do
      it "should supply value to the function" $ do
        sl <- SL.new
        SL.insert sl "foo" "bar"
        resFocus <- SL.focus sl "foo" (\v -> (v, Keep))
        resFocus `shouldBe` Just "bar"

      it "should supply Nothing if value doesnt exist" $ do
        sl <- SL.new :: IO (SL.SkipList String String)
        resFocus <- SL.focus sl "foo" (\v -> (v, Keep))
        resFocus `shouldBe` Nothing

      it "should keep value" $ do
        sl <- SL.new
        SL.insert sl "foo" "bar"
        resFocus <- SL.focus sl "foo" (\_ -> ("barbar", Keep))
        resLookup <- SL.lookup sl "foo"
        resFocus `shouldBe` "barbar"
        resLookup `shouldBe` Just "bar"

      it "should delete value" $ do
        sl <- SL.new
        SL.insert sl "foo" "bar"
        resFocus <- SL.focus sl "foo" (\_ -> ("barbar", Remove))
        resLookup <- SL.lookup sl "foo"
        resFocus `shouldBe` "barbar"
        resLookup `shouldBe` Nothing

      it "should update value" $ do
        sl <- SL.new
        SL.insert sl "foo" "bar"
        resFocus <- SL.focus sl "foo" (\_ -> ("barbar", Replace "barbarbar"))
        resLookup <- SL.lookup sl "foo"
        resFocus `shouldBe` "barbar"
        resLookup `shouldBe` Just "barbarbar"

      it "should create new value" $ do
        sl <- SL.new
        resFocus <- SL.focus sl "foo" (\_ -> ("barbar", Replace "barbarbar"))
        resLookup <- SL.lookup sl "foo"
        resFocus `shouldBe` "barbar"
        resLookup `shouldBe` Just "barbarbar"
