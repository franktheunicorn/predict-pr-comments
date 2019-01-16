package suggester

import "github.com/google/go-github/github"

type Client struct {
}

type ClientOptions struct {
}

func NewClient() (*Client, error) {

	return &Client{}, nil
}
