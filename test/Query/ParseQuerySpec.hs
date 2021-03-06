module Query.ParseQuerySpec
  (
    main
  , spec
  ) where

import Test.Hspec
import Data.ByteString.Char8 as B
import Query

main :: IO ()
main = hspec spec

spec :: Spec
spec = do
  describe "parseQuery" $ do
    it "should correctly parse quoted arguments" $ do
      parseQuery (B.pack "set \"test test\" \"test test\"")
      `shouldBe` Just (Set (B.pack "test test") (B.pack "test test"))

    it "should ignore value if quote isn't closed" $ do
      parseQuery (B.pack "get \"test test\" \"test test")
      `shouldBe` Just (Get [(B.pack "test test")])

    it "should not ignore value without space or newline at the end" $ do
      parseQuery (B.pack "set test test")
      `shouldBe` Just (Set (B.pack "test") (B.pack "test"))

    it "should ignore unquoted trailing whitespace" $ do
      parseQuery (B.pack "set test test    ")
      `shouldBe` Just (Set (B.pack "test") (B.pack "test"))

    it "should not ignore quoted trailing whitespace" $ do
      parseQuery (B.pack "set test \"test    \"")
      `shouldBe` Just (Set (B.pack "test") (B.pack "test    "))

    it "should parse command case-insensitivly" $ do
      parseQuery (B.pack "SET test test")
      `shouldBe` Just (Set (B.pack "test") (B.pack "test"))

    it "should parse get command with one argument" $ do
      parseQuery (B.pack "get test")
      `shouldBe` Just (Get [(B.pack "test")])

    it "should parse mulptiple get arguments" $ do
      parseQuery (B.pack "get test1 test2 test3")
      `shouldBe` Just (Get [B.pack "test1", B.pack "test2", B.pack "test3"])

    it "should parse set command with two arguments" $ do
      parseQuery (B.pack "set test test")
      `shouldBe` Just (Set (B.pack "test") (B.pack "test"))

    it "should not parse set command with more than two arguments" $ do
      parseQuery (B.pack "SET test test test test")
      `shouldBe` Nothing

    it "should not parse set command with less than two arguments" $ do
      parseQuery (B.pack "SET test")
      `shouldBe` Nothing

    it "should parse delete command with one argument" $ do
      parseQuery (B.pack "delete test")
      `shouldBe` Just (Delete (B.pack "test"))

    it "should not parse delete command with more than one argument" $ do
      parseQuery (B.pack "delete test test")
      `shouldBe` Nothing

    it "should parse add command with two arguments" $ do
      parseQuery (B.pack "add test test")
      `shouldBe` Just (Add (B.pack "test") (B.pack "test"))

    it "should not parse add command with more than two arguments" $ do
      parseQuery (B.pack "add test test test")
      `shouldBe` Nothing

    it "should not parse add command with less than two arguments" $ do
      parseQuery (B.pack "add test")
      `shouldBe` Nothing

    it "should parse replace command with two arguments" $ do
      parseQuery (B.pack "replace test test")
      `shouldBe` Just (Replace (B.pack "test") (B.pack "test"))

    it "should not parse replace command with more than two arguments" $ do
      parseQuery (B.pack "replace test test test")
      `shouldBe` Nothing

    it "should not parse replace command with less than two arguments" $ do
      parseQuery (B.pack "replace test")
      `shouldBe` Nothing

    it "should parse incr command with integer as a second argument" $ do
      parseQuery (B.pack "incr test 5")
      `shouldBe` Just (Incr (B.pack "test") 5)

    it "should not parse incr command with string as a second argument" $ do
      parseQuery (B.pack "incr test abaaba")
      `shouldBe` Nothing

    it "should not parse incr command with more than two arguments" $ do
      parseQuery (B.pack "incr test 5 7")
      `shouldBe` Nothing

    it "should not parse incr command with less than two arguments" $ do
      parseQuery (B.pack "incr test")
      `shouldBe` Nothing

    it "should parse decr command with integer as a second argument" $ do
      parseQuery (B.pack "decr test 5")
      `shouldBe` Just (Decr (B.pack "test") 5)

    it "should not parse decr command with string as a second argument" $ do
      parseQuery (B.pack "decr test abaaba")
      `shouldBe` Nothing

    it "should not parse decr command with more than two arguments" $ do
      parseQuery (B.pack "decr test 5 7")
      `shouldBe` Nothing

    it "should not parse decr command with less than two arguments" $ do
      parseQuery (B.pack "decr test")
      `shouldBe` Nothing

    it "should parse append command with two arguments" $ do
      parseQuery (B.pack "append test test")
      `shouldBe` Just (Append (B.pack "test") (B.pack "test"))

    it "should not parse append command with more than two arguments" $ do
      parseQuery (B.pack "append test test test test")
      `shouldBe` Nothing

    it "should not parse append command with less than two arguments" $ do
      parseQuery (B.pack "append test")
      `shouldBe` Nothing

    it "should parse prepend command with two arguments" $ do
      parseQuery (B.pack "prepend test test")
      `shouldBe` Just (Prepend (B.pack "test") (B.pack "test"))

    it "should not parse prepend command with more than two arguments" $ do
      parseQuery (B.pack "prepend test test test test")
      `shouldBe` Nothing

    it "should not parse prepend command with less than two arguments" $ do
      parseQuery (B.pack "prepend test")
      `shouldBe` Nothing
