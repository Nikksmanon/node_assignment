const Express = require("express");
const BodyParser = require("body-parser");
const { MongoClient } = require("mongodb");
const dotenv = require("dotenv");
const app = Express();
dotenv.config();
const { DATABASE_HOST, DATABASE_PORT, DATABASE_NAME } = process.env;
// Access Db using .env file
let { DATABASE_URL } = process.env;
console.log("db data", DATABASE_HOST, DATABASE_PORT, DATABASE_NAME);
DATABASE_URL = `mongodb://${DATABASE_HOST}:${DATABASE_PORT}/${DATABASE_NAME}`;
console.log("db connection success", DATABASE_URL);
app.use(BodyParser.json());
app.use(BodyParser.urlencoded({ extended: true }));

let database, collection;
// Setup Db connection 
app.listen(5000, () => {
  MongoClient.connect(
    DATABASE_URL,
    { useUnifiedTopology: true },
    (error, client) => {
      if (error) {
        throw error;
      }
      database = client.db(DATABASE_NAME);
      collection = database.collection("authors");
      booksCollection = database.collection("books");
      pricesCollection = database.collection("prices");
      console.log("Connected to `" + DATABASE_NAME + "`!");
    }
  );
});
// list all authors
app.get("/authors", (req, res) => {
  collection.find({}).toArray((error, result) => {
    if (error) {
      return res.status(500).send(error);
    }
    res.send(result);
  });
});
// Create GET api to fetch authors who have greater than or equal to n awards
app.get("/authors/awards/:n", (req, res) => {
  collection
    .find({ awards: { $gte: parseInt(req.params.n) } })
    .toArray((error, result) => {
      if (error) {
        return res.status(500).send(error);
      }
      res.send(result);
    });
});
// Create GET api to fetch authors who have won award where year >= y
app.get("/authors/year/:y", (req, res) => {
  console.log(new Date(req.params.y))  
  collection
    .find({ year: { $gte: new Date(req.params.y).toISOString() } })
    .toArray((error, result) => {
      if (error) {
        return res.status(500).send(error);
      }
      res.send(result);
    });
});
// Create GET api to fetch total number of books sold and total profit by each author
app.get("/checkout", (req, res) => {
  booksCollection
    .aggregate([
      { $match: {} },
      {
        $lookup: {
          from: "authors",
          localField: "authorId",
          foreignField: "_id",
          as: "authors",
        },
      },
      { $unwind: "$authors" },
      {
        $lookup: {
          from: "prices",
          localField: "_id",
          foreignField: "bookId",
          as: "prices",
        },
      },
      { $unwind: "$prices" },
      {
        $group: {
          _id: "$authors._id",
          totalBooksSold: { $sum: "$booksSold" },
          totalProfit: { $sum: { $multiply: ["$booksSold", "$prices.price"] } },
        },
      },
    ])
    .toArray((error, result) => {
      if (error) {
        return res.status(500).send(error);
      }
      res.send(result);
    });
});

// Create GET api which accepts parameter birthDate and totalPrice, where birthDate is date string and totalPrice is number
app.get("/filter", (req, res) => {
  const query = {
    $and: [
      {
        birthDate: { $gte: new Date(req.query.birthDate) },
      },
      {
        totalPrice: { $gte: parseInt(req.query.totalPrice) },
      },
    ],
  };
  
  collection
    .aggregate([
      {
        $lookup: {
          from: "prices",
          localField: "_id",
          foreignField: "authorId",
          as: "prices",
        },
      },
      { $unwind: "$prices" },
      {
        $group: {
          _id: "$_id",
          totalPrice: { $sum: "$prices.price" },
          birthDate: { $first: "$birthDate" },
        },
      },
      {
        $match: query,
      },
    ])
    .toArray((error, result) => {
      if (error) {
        return res.status(500).send(error);
      }
      res.send(result);
    });
});

// all data insert queries
app.post("/authors", (req, res) => {
  collection.insertMany(req.body, (error, result) => {
    if (error) {
      return res.status(500).send(error);
    }
    res.send(result.result);
  });
});

app.post("/books", (req, res) => {
  booksCollection.insertMany(req.body, (error, result) => {
    if (error) {
      return res.status(500).send(error);
    }
    res.send(result.result);
  });
});

app.post("/prices", (req, res) => {
  pricesCollection.insertMany(req.body, (error, result) => {
    if (error) {
      return res.status(500).send(error);
    }
    res.send(result.result);
  });
});
