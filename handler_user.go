package main

import (
	"encoding/json"
	"fmt"
	"net/http"
)

func (apiCfg *apiConfig) handlerCreateUser(w http.ResponseWriter, r *http.Request) {
	type parameters struct {
		Name string `name`

	}
	decoder := json.NewDecoder(r.Body)

	params := parameters{}
	err := decoder.Decode(&params)
	if err != nil {
		respondWithError(w, 400, fmt.Sprintf("Error parsing JSON:", err) )
		return
	}

	apiCfg.DB.CreateUser(r.Context(), database.CreateUserParams{
		ID: 
	})
	
	respondWithJSON(w, 200, struct{}{})
}