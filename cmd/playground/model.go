package main

// Domain structs for the playground.
// Fields are tagged with `db` so they work with db.InsertStruct / vapordb.ScanRows.

type User struct {
	ID    int     `db:"id"`
	Name  string  `db:"name"`
	Age   int     `db:"age"`
	Score float64 `db:"score"` // 0 when NULL in the DB
}

type Order struct {
	ID      int     `db:"id"`
	UserID  int     `db:"user_id"`
	Product string  `db:"product"`
	Amount  float64 `db:"amount"`
}

// ── manual mapping example (commented out — uncomment to try) ─────────────────
//
// If you need custom NULL handling or want to map columns to nested fields,
// write a rowToX function by hand instead of using ScanRows:
//
// func rowToUser(r vapordb.Row) User {
// 	u := User{}
// 	if v := r["id"]; v.Kind != vapordb.KindNull {
// 		u.ID = int(v.V.(int64))
// 	}
// 	if v := r["name"]; v.Kind != vapordb.KindNull {
// 		u.Name = v.V.(string)
// 	}
// 	if v := r["age"]; v.Kind != vapordb.KindNull {
// 		u.Age = int(v.V.(int64))
// 	}
// 	if v := r["score"]; v.Kind != vapordb.KindNull {
// 		switch n := v.V.(type) {
// 		case float64:
// 			u.Score = n
// 		case int64:
// 			u.Score = float64(n)
// 		}
// 	}
// 	return u
// }
