# auth
Contains a basic Auth provider that can be used for simple user management and topic restrictions.

```
func main() {
	a := auth.NewBasicAuth()
	a.ToggleGuestAccess(true)
	a.RegisterUser("roan", "roan", "brand")
	a.AllowSubscription("testtopic/#", "roan")
	a.AllowPublish("testtopic", "roan")

	s := gobroke.Server{Auther: a}
	err := s.Run()
	if err != nil {
		panic(err)
	}
}
```

## Extending
* The `Auther` interface can be used to easily implement wrappers for external auth systems. See `interface.go`
