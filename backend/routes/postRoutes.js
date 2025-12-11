import express from "express";
import {
  createPost,
  getPostById,
  getPostsByUser,
  getAllPosts,
  getUserFeed,
} from "../controllers/postController.js";
import { asyncHandler } from "../middleware/errorHandler.js";

const router = express.Router();

router.post("/", asyncHandler(createPost));
router.get("/", asyncHandler(getAllPosts));
router.get("/feed/:user_id", asyncHandler(getUserFeed)); // Must be before /:id to avoid route conflicts
router.get("/user/:user_id", asyncHandler(getPostsByUser));
router.get("/:id", asyncHandler(getPostById));

export default router;
