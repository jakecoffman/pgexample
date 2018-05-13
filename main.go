package main

import (
	"github.com/gin-gonic/gin"
	"log"
	_ "github.com/lib/pq"
	"github.com/jmoiron/sqlx"
)

func main() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	const schema = `
-- drop table if exists public.preference;
-- drop table if exists public.user;

create table if not exists public.user (
	ID int unique generated always as identity,
	NAME text not null
);

create table if not exists public.preference (
	ID int unique generated always as identity,
	USER_ID int,
	NAME text unique not null,
	VALUE text not null,
	constraint preference_user_fk foreign key (user_id) references "user" (id)
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

	{
		// todo make these functions that take params that return the proper sql (limits, wheres, etc)
		const listUser = `select * from public.user`
		const addUser = `insert into public.user (name) values (:name) RETURNING id`
		engine.GET("/users", list(db, listUser))
		engine.POST("/users", add(db, addUser))
	}

	{
		const listPrefs = `select * from public.preference`
		const addPref = `insert into public.preference (user_id, name, value) values (:user_id, :name, :value)`
		engine.GET("/prefs", list(db, listPrefs))
		engine.POST("/prefs", add(db, addPref))
	}

	err = engine.Run("0.0.0.0:8999")
	if err != nil {
		log.Fatal(err)
	}
}

type Err struct {
	Error string
}

func list(db *sqlx.DB, query string) func(ctx *gin.Context) {
	return func(ctx *gin.Context) {
		user, err := getArray(db, query)
		if err != nil {
			log.Println(err)
			ctx.JSON(500, Err{err.Error()})
			return
		}

		ctx.JSON(200, user)
	}
}

func add(db *sqlx.DB, query string) func(ctx *gin.Context) {
	return func(ctx *gin.Context) {
		js := map[string]interface{}{}
		err := ctx.ShouldBindJSON(&js)
		if err != nil {
			log.Println(err)
			ctx.JSON(500, Err{err.Error()})
			return
		}

		rows, err := db.NamedQuery(query, js)
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

func getArray(db *sqlx.DB, query string) ([]map[string]interface{}, error) {
	rows, err := db.Query(query)
	if err != nil {
		log.Println(err)
		return nil, err
	}
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