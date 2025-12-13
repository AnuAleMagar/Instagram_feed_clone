import express from "express";
import {
  createUser,
  getAllUsers,
  getUserById,
  followUser,
  unfollowUser,
} from "../controllers/userController.js";

const router = express.Router();

router.post("/", createUser);
router.get("/", getAllUsers);
router.post("/:id/follow", followUser);
router.post("/:id/unfollow", unfollowUser);
router.get("/:id", getUserById);

export default router;
