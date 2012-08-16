# AZQL

AZQL is a SQL like DSL for Clojure.

Main goals of this project:

- no restrictions by schema, orm-mappings etc.
- DSL should be closer to native SQL as much as possible
- no fake optimizations (modern DBs are clever enough)
- protection from injections, but not from invalid queries
- sometimes we want to include vendor-specefic parts into queries


## Installation

Add the following to your project.clj:

	[azql "0.1.0-SNAPSHOT"]


## Usage

### Basic examples

AZQL syntax is quite similar to SQL:

	(select
	  (fields [:id :name :email])
	  (from :Users)
	  (where (= :role "ADMIN"))
	  (fetch-all))

After macroexpansions:

	(->
	  (select*)
	  (fields* {:id :id, :name :name, :email :email})
	  (join* nil :Users :Users) ; nil means 'implicit cross join'
	  (where* ['= :role "ADMIN"])
	  (fetch-all))

Actual work is doing in `fetch-all` - this function executes query
and converts `resultset-seq` into vector.

Library provides some alternatives:
- `fetch-one` feches only one record, raises exception if more than one recerd is returened.
- `fetch-single` fetches only single value (one row and one column), useful for aggregate queries
- `with-fetch` executes arbitary code with open `resultset-seq`

Example:

	(with-fetch [f (select (from :Users))]
	  ; f it seq of rows
	  (reduce + (map :rating f)))

It is possible to compose additional where's:

	(def all-users (select (from :Users)))
	(def banned-users (-> all-users (where (= :status "BANNED"))))
	(def banned-admins (-> banned-users (where (= :role "ADMIN")))
	(println (fetch-all banned-admins))

The actual SQL is available trough the function `sql`:

	user> (sql (select (from :Users) (where (= :id 123))))
	#azql.emit.Sql{:sql "SELECT * FROM \"Users\" WHERE (\"id\" = ?)", :args [123]}


### Joins

AZQL supports all types of joins:

	(select
	  (from :a :A)                      ; table 'A', alias 'a', implicit cross join
	  (join :b :B (= :a.x :b.y))        ; inner join
	  (join-cross :c :B)                ; explicit cross join
	  (join-inner :D (= :a.x :D.y))
	  (join-full :e :TableE (and (= :e.x :a.x) (:e.y :D.y))))

It is possible to use vendor-specific joins:

	(select
	  (from :a :TableOne)
	  (join* 'COOL_JOIN :b :TableTwo (= :b.x = :a.y)))


### Ordering

You can use ordering:

	(select
	  (from :A)
	  (order :field1)
	  (order :field2 :desc)
	  (order (+ :x :y) :asc))

It will produce 'SELECT * FROM "A" ORDER BY ("x" + "y") ASC, "field2" DESC, "field1"'.


### Grouping

AZQL supports grouping:

	(select
	  (fields {:name :u.name})
	  (from :u :Users)
	  (join :p :Posts (= :p.userid :u.id))
	  (group :u.name)
	  (having (> 10 (count :p.id))))

You *must* specify aliases for all columns.


### Subqueries

TODO

### Raw queries

TODO


## Table manipulation functions

### Inserts

TODO


### Updates

TODO


### Deletes

TODO


## License

Copyright © 2012 Andrei Zhlobich

Distributed under the Eclipse Public License, the same as Clojure.
