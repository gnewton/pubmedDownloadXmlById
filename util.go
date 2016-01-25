package main

import (
	"time"
)

const NewYorkCityESTLocation = "America/New_York"

func isReducedLoadTime() (bool, error) {
	t := time.Now()
	utc, err := time.LoadLocation(NewYorkCityESTLocation)
	if err != nil {
		return false, err
	}
	nyct := t.In(utc)
	return afterHours2(&nyct) || isWeekend(&nyct), nil
}

func afterHours2(t *time.Time) bool {
	h := t.Hour()
	return h < 9 || h > 17

}

func isWeekend(t *time.Time) bool {
	d := t.Day()
	return d == 0 || d == 6
}
