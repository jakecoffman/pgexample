package main

import (
	"github.com/gin-gonic/gin"
	"log"
	"database/sql"
	_ "github.com/lib/pq"
	"github.com/jmoiron/sqlx"
)

func main() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	const schema = `
create table if not exists public.user (
	ID int generated always as identity,
	NAME text not null
);

create table if not exists public.preference (
	ID int generated always as identity,
	NAME text unique not null,
	VALUE text not null
);
`

	db, err := sqlx.Open("postgres", "user=postgres dbname=postgres sslmode=disable")
	if err != nil {
		log.Fatal(err)
	}

	go func() {
		_, err = db.Exec(schema)
		if err != nil {
			log.Fatal(err)
		}
	}()

	engine := gin.Default()
	engine.GET("/", func (ctx *gin.Context) {
		ctx.Header("Content-Type", "text/html")
		ctx.String(200, `<html><a href="/users">Users</a></html>`)
	})
	engine.GET("/users", listUsers(db))
	engine.POST("/users", addUser(db))
	err = engine.Run("0.0.0.0:8999")
	if err != nil {
		log.Fatal(err)
	}
}

type Err struct {
	Error string
}

func listUsers(db *sqlx.DB) func(ctx *gin.Context) {
	return func(ctx *gin.Context) {
		rows, err := db.Query("select * from public.user")
		if err != nil {
			log.Println(err)
			ctx.JSON(500, Err{err.Error()})
			return
		}

		js, err := getJSON(rows)
		if err != nil {
			log.Println(err)
			ctx.JSON(500, Err{err.Error()})
			return
		}

		ctx.Header("Content-Type", "application/json")
		ctx.JSON(200, js)
	}
}

func addUser(db *sqlx.DB) func(ctx *gin.Context) {
	return func(ctx *gin.Context) {
		js := map[string]interface{}{}
		err := ctx.ShouldBindJSON(&js)
		if err != nil {
			log.Println(err)
			ctx.JSON(500, Err{err.Error()})
			return
		}

		rows, err := db.NamedQuery("insert into public.user (name) values (:name) RETURNING id", js)
		if err != nil {
			log.Println(err)
			ctx.JSON(500, Err{err.Error()})
			return
		}
		defer rows.Close()

		rows.Next()
		var id int
		if err = rows.Scan(&id); err != nil {
			log.Println(err)
			ctx.JSON(500, Err{err.Error()})
			return
		}

		js["id"] = id
		ctx.JSON(201, js)
	}
}

func getJSON(rows *sql.Rows) ([]map[string]interface{}, error) {
	defer rows.Close()
	columns, err := rows.Columns()
	if err != nil {
		return nil, err
	}
	count := len(columns)
	tableData := make([]map[string]interface{}, 0)
	values := make([]interface{}, count)
	valuePtrs := make([]interface{}, count)
	for rows.Next() {
		for i := 0; i < count; i++ {
			valuePtrs[i] = &values[i]
		}
		rows.Scan(valuePtrs...)
		entry := make(map[string]interface{})
		for i, col := range columns {
			var v interface{}
			val := values[i]
			b, ok := val.([]byte)
			if ok {
				v = string(b)
			} else {
				v = val
			}
			entry[col] = v
		}
		tableData = append(tableData, entry)
	}
	return tableData, nil
}