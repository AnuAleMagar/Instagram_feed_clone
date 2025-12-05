import { DataTypes } from "sequelize";
import { sequelize } from "../config/db.js";

const User = sequelize.define(
  "User",
  {
    id: {
      type: DataTypes.INTEGER,
      primaryKey: true,
      autoIncrement: true,
    },
    username: {
      type: DataTypes.TEXT,
      allowNull: false,
      unique: true,
      validate: {
        notEmpty: true,
      },
    },
    email: {
      type: DataTypes.TEXT,
      allowNull: false,
      unique: true,
      validate: {
        isEmail: true,
      },
    },
    bio: {
      type: DataTypes.TEXT,
      allowNull: true,
    },
    avatar_url: {
      type: DataTypes.TEXT,
      allowNull: true,
    },
    followers_count: {
      type: DataTypes.INTEGER,
      defaultValue: 0,
      validate: {
        min: 0,
      },
    },
    following_count: {
      type: DataTypes.INTEGER,
      defaultValue: 0,
      validate: {
        min: 0,
      },
    },

    is_celebrity: {
      type: DataTypes.BOOLEAN,
      defaultValue: false,
    },
  },
  {
    tableName: "users",
    timestamps: false, // We're using created_at manually
    indexes: [
      {
        unique: true,
        fields: ["username"],
      },
      {
        unique: true,
        fields: ["email"],
      },
      {
        fields: ["is_celebrity"],
      },
    ],
  }
);

export default User;
