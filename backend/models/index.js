import User from "./User.js";
import Follow from "./Follow.js";
import Post from "./Post.js";

Post.belongsTo(User, {
  foreignKey: "user_id",
  as: "user",
});

User.hasMany(Post, {
  foreignKey: "user_id",
  as: "posts",
}); 


User.belongsToMany(User, {
  through: Follow,
  foreignKey: "follower_id",
  otherKey: "following_id",
  as: "following",
});

User.belongsToMany(User, {
  through: Follow,
  foreignKey: "following_id",
  otherKey: "follower_id",
  as: "followers",
});

export { User, Follow, Post };
