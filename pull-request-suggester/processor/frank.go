package processor

import (
	"fmt"
	"github.com/kubicorn/kubicorn/pkg/rand"
)

func GetFrankMessage() string {
	gl := len(greeting)
	sl := len(suspect)
	gj := greeting[rand.GenerateRandomInt(0, gl)]
	sj := suspect[rand.GenerateRandomInt(0, sl)]

	return fmt.Sprintf("%s %s", gj, sj)
}

var greeting = []string{
	"Henlo friend. 🦄",
	"Hrmmm...",
	"Hi, just checking in.",
	"Hi...🦄 So I read your PR...",
	"Me again, just one more nit...🌈",
	"Nit... but...",
}

var suspect = []string{
	"Something looks weird here. Are you sure about this? 🌈",
	"Woah... this old bit again... 🦄",
	"So... are you sure about this 🐼 pandas?",
	"Meeps... this looks strange...",
	"Meeps Meeps... this looks weird...",
	"Hrmm.. this looks funny...",
	"Frank is confused... 🐼",
	"Frank is confused... 🌈🐼",
	"Are you sure about this?⚡",
	"Wondering why this is here...? 🌈🐼",
	"So maybe take a look here? ⚡🐼",
}
