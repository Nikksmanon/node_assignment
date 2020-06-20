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
    .aggregate([
      {
        $project: {
          name: "$name",
          awards_count: { $size: { $ifNull: ["$awards", []] } },
        },
      },
      { $match: { awards_count: { $gte: parseInt(req.params.n) } } },
    ])
    .toArray((error, result) => {
      if (error) {
        return res.status(500).send(error);
      }
      res.send(result);
    });
});
// Create GET api to fetch authors who have won award where year >= y
app.get("/authors/year/:y", (req, res) => {
  collection
    .find({ "awards.year": { $gte: parseInt(req.params.y) } })
    .toArray((error, result) => {
      if (error) {
        return res.status(500).send(error);
      }
      res.send(result);
    });
});
// Create GET api to fetch total number of books sold and total profit by each author
app.get("/checkout", (req, res) => {
  collection
    .aggregate([
      {
        $lookup: {
          from: "books",
          localField: "_id",
          foreignField: "authorId",
          as: "books",
        },
      },
      { $unwind: { path: "$books", preserveNullAndEmptyArrays: true } },
      {
        $group: {
          _id: "$_id",
          totalBooksSold: { $sum: "$books.sold" },
          totalProfit: { $sum: { $multiply: ["$books.price", "$books.sold"] } },
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
          from: "books",
          localField: "_id",
          foreignField: "authorId",
          as: "books",
        },
      },
      { $unwind: { path: "$books", preserveNullAndEmptyArrays: true } },

      {
        $group: {
          _id: "$_id",
          totalPrice: { $sum: "$books.price" },
          birthDate: { $first: "$birth" },
        },
      },

      {
        $match: query,
      },
      {
        $project: {
          _id: "$_id",
          birthDate: "$birthDate",
          totalPrice: "$totalPrice",
        },
      },
      { $sort: { _id: 1 } },
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
