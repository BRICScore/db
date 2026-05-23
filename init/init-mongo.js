db = db.getSiblingDB("brics");
db.createUser({
  user: process.env.MONGO_BRICS_ADMIN_USERNAME,
  pwd: process.env.MONGO_BRICS_ADMIN_PASSWORD,
  roles: [{ role: "dbAdmin", db: "brics" }],
});
db.createUser({
  user: process.env.MONGO_BRICS_USER_USERNAME,
  pwd: process.env.MONGO_BRICS_USER_PASSWORD,
  roles: [{ role: "readWrite", db: "brics" }],
});
