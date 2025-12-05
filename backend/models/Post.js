import { DataTypes } from "sequelize";
import { sequelize } from "../config/db.js";

const Post = sequelize.define(
  "Post",
  {
    id: {
      type: DataTypes.UUID,
      defaultValue: DataTypes.UUIDV4,
      primaryKey: true,
    },
    user_id: {
      type: DataTypes.INTEGER,
      allowNull: false,
      references: {
        model: "users",
        key: "id",
      },
      onDelete: "CASCADE",
    },
    caption: {
      type: DataTypes.TEXT,
      allowNull: true,
    },
    image_url: {
      type: DataTypes.TEXT,
      allowNull: false,
    },
    likes_count: {
      type: DataTypes.INTEGER,
      defaultValue: 0,
      validate: {
        min: 0,
      },
    },
    comments_count: {
      type: DataTypes.INTEGER,
      defaultValue: 0,
      validate: {
        min: 0,
      },
    },
    created_at: {
      type: DataTypes.DATE,
      defaultValue: DataTypes.NOW,
    },
  },
  {
    tableName: "posts",
    timestamps: false,
    indexes: [
        {
            fields: ["user_id", "created_at"],
            name: "idx_posts_user_created",
          },
          {
            fields: ["created_at", "id"],
            name: "idx_posts_created_id",
          },
    ],
  }
);

export default Post;        



