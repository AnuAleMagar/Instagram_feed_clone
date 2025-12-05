import { DataTypes } from "sequelize";
import { sequelize } from "../config/db.js";

const Follow = sequelize.define(
  "Follow",
  {
    id: {
      type: DataTypes.INTEGER,
      primaryKey: true,
      autoIncrement: true,
    },
    follower_id: {
      type: DataTypes.INTEGER,
      allowNull: false,
      references: {
        model: "users",
        key: "id",
      },
      onDelete: "CASCADE",
    },
    following_id: {
      type: DataTypes.INTEGER,
      allowNull: false,
      references: {
        model: "users",
        key: "id",
      },
      onDelete: "CASCADE",
    },
    created_at: {
      type: DataTypes.DATE,
      defaultValue: DataTypes.NOW,
    },
  },
  {
    tableName: "follows",
    timestamps: false,
    indexes: [
      {
        fields: ["follower_id"],
      },
      {
        fields: ["following_id"],
      },
      {
        unique: true,
        fields: ["follower_id", "following_id"],
        name: "unique_follow",
      },
    ],
  }
);

export default Follow;
