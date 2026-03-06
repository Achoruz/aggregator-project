package main

import (
	"aggregator-project/internal/db"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/go-chi/chi"
	"github.com/google/uuid"
)

func (apiCfg *apiConfig) handlerCreateFeedFollow(w http.ResponseWriter, r *http.Request, user db.User) {
	type parameters struct {
		FeedID uuid.UUID `json:"feed_id"`
	}
	decoder := json.NewDecoder(r.Body)

	params := parameters{}
	err := decoder.Decode(&params)
	if err != nil {
		respondWithError(w, 400, fmt.Sprintf("Error parsing JSON: %v", err) )
		return
	}

	feedFollow, err := apiCfg.DB.CreateFeedFollow(r.Context(), db.CreateFeedFollowParams{
		ID: uuid.New(),
		CreatedAt: time.Now().UTC(),
		UpdatedAt: time.Now().UTC(),
		UserID: user.ID,
		FeedID: params.FeedID,
	})
	if err != nil {
		respondWithError(w, 400, fmt.Sprintf("Error creating feed follow: %v", err))
		return
	}
	
	respondWithJSON(w, 201, databaseFeedFollowToFeedFollow(feedFollow))
}

func (apiCfg *apiConfig) handlerGetFeedFollows(w http.ResponseWriter, r *http.Request, user db.User) {
	feedFollows, err := apiCfg.DB.GetFeedFollows(r.Context(), user.ID)
	if err != nil {
		respondWithError(w, 400, fmt.Sprintf("Couldn't get feed follows: %v", err))
		return
	}
	
	respondWithJSON(w, 201, databaseFeedFollowsToFeedFollows(feedFollows))
}

func (apiCfg *apiConfig) handlerDeleteFeedFollows(w http.ResponseWriter, r *http.Request, user db.User) {
	feedFollowIDStr := chi.URLParam(r, "feedFollowID")
	feedFollowID, err := uuid.Parse(feedFollowIDStr)
	if err != nil {
		respondWithError(w, 400, fmt.Sprintf("Couldn't parse feed follows id: %v", err))
		return
	}

	err = apiCfg.DB.DeleteFeedFollow(r.Context(), db.DeleteFeedFollowParams{
		ID: feedFollowID,
		UserID: user.ID,
	})
	if err != nil {
		respondWithError(w, 400, fmt.Sprintf("Couldn't delete feed follow id: %v", err))
		return
	}
	respondWithJSON(w, 200, struct{}{})
}


