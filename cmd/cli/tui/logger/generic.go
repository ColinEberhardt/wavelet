package logger

import (
	"fmt"
	"strings"
	"time"

	"github.com/diamondburned/tview/v2"
)

type generic struct {
	time time.Time
	with stringmap

	// stuff that changes with different levels
	color string // goes to [color::]
}

func newGeneric(color string) *generic {
	return &generic{
		time:  time.Now(),
		color: color,
	}
}

func (g *generic) f(key, format string, values ...interface{}) {
	g.with.set(tview.Escape(key), tview.Escape(fmt.Sprintf(format, values...)))
}

func (g *generic) wrapShort(content string) string {
	return g.time.Format(time.Kitchen) + " - [" + g.color + "]" + content + "[-]"
}

func (g *generic) shortkeys() string {
	return g.keys("", ", ", 8)
}

func (g *generic) wrapFull(content string) string {
	return g.time.Format(time.Stamp) + " - [" + g.color + "]" + content + "[-]"
}

func (g *generic) fullkeys() string {
	return g.keys("\t", "\n", 0)
}

func (g *generic) keys(pre, suf string, trim int) string {
	var b strings.Builder

	for _, s := range g.with.get() {
		var val = s[1]

		if trim > 0 && len(s[1]) > trim {
			val = val[:trim] + "…"
		}

		b.WriteString(
			"[-]" + pre + s[0] + " = " + val + "[-]" + suf,
		)
	}

	str := b.String()
	return str[:len(str)-len(suf)]
}