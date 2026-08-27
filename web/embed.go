package web

import "embed"

// Assets contains the dependency-free demonstration console.
//
//go:embed index.html style.css app.js
var Assets embed.FS
