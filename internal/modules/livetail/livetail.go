package livetail

type Hub interface {
	Subscribe(teamID int64, ch chan any, filter func(any) bool) bool
	Unsubscribe(teamID int64, ch chan any)
	Publish(teamID int64, event any)
}
