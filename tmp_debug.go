package main

import (
"fmt"
"os"

"github.com/observability/observability-backend-go/internal/defaultconfig"
)

func main() {
	fsys := os.DirFS("internal/defaultconfig")
	registry, err := defaultconfig.LoadFromFS(fsys)
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}
	
	bytes, err := registry.GenerateDefaultDashboardConfigsJSON()
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}
	
	fmt.Println(bytes[:200], "...")
}
