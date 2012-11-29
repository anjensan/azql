# AZQL

AZQL is a SQL like DSL for Clojure.

Main goals of this project:

- no restrictions by schema, orm-mappings etc.
- DSL should be closer to native SQL as much as possible
- no fake optimizations (modern DBs are clever enough)
- protection from injections, but not from invalid queries
- sometimes we want to include vendor-specific parts into queries


## Installation

Add the following to your project.clj:

    :::clojure
    [azql "0.1.0-SNAPSHOT"]


## Usage

### Basic examples

AZQL syntax is quite similar to SQL:

    :::clojure
    (select
      (fields [:id :name :email])
      (from :a :Users)
      (where (= :a.role "ADMIN"))
      (fetch-all))

After macroexpansions:

    :::clojure
    (->
      (select*)
      (fields* {:id :id, :name :name, :email :email})
      (join* nil :a :Users) ; nil means 'implicit cross join'
      (where* (list '= :a.role "ADMIN"))
      (fetch-all))

Actual work is doing in `fetch-all` - this function executes query
and converts `resultset-seq` into vector.

Library provides some alternatives:

- `fetch-one` feches only one record, raises exception if more than one record is returened
- `fetch-single` fetches only single value (one row and one column)
- `with-fetch` executes arbitary code with opened `resultset-seq`

Example:

    :::clojure
    (with-fetch [f (table :Users)]
      (reduce + (map :rating f)))

It is possible to compose additional where's:

    :::clojure
    (def all-users (table :Users))
    (def banned-users (-> all-users (where (= :status "BANNED"))))
    (def banned-admins (-> banned-users (where (= :role "ADMIN")))
    (println (fetch-all banned-admins))

Also you can use map-style conditions.

    :::clojure
    (select
      (from :Users)
      (where {:first "Ivan", :last "Ivanov"}))

The actual SQL is available trough the function `sql`:

    :::clojure
    user> (select (from :Users) (where {:id 123}) (sql))
    #<"SELECT * FROM \"Users\" WHERE (\"id\" = ?)" 123>


### Joins

AZQL supports all types of joins:

    :::clojure
    (select
      (from :a :A)                      ; table 'A', alias 'a', implicit cross join
      (join :b :B (= :a.x :b.y))        ; inner join
      (join-cross :c :B)                ; explicit cross join
      (join-inner :D {:a.x :D.y})
      (join-full :e :E {:e.x :a.x, :e.y :D.y)))

The only restriction is that first join must be 'implicit cross join' (function `from`).
It is possible to use vendor-specific joins:

    :::clojure
    (select
      (from :a :TableOne)
      (join* (raw "COOL JOIN") :b :TableTwo (= :b.x = :a.y)))


### Ordering

You can use ordering:

    :::clojure
    (select
      (from :A)
      (order :field1)
      (order :field2 :desc)
      (order (+ :x :y) :asc))

It will produce 'SELECT * FROM "A" ORDER BY ("x" + "y") ASC, "field2" DESC, "field1"'.


### Grouping

AZQL supports grouping:

    :::clojure
    (select
      (fields {:name :u.name})
      (from :u :Users)
      (join :p :Posts (= :p.userid :u.id))
      (group :u.name)
      (having (> 10 (count :p.id))))


### Subqueries

Library supports subqueries:

    :::clojure
    (def all-users (select (from :Users)))
    (def all-active-users (select (from all-users) (where (= :status "ACTIVE"))))
    (fetch-all all-active-users)


ALL, ANY and SOME are supported also.

    :::clojure
    (select
      (from :u :Users)
      (where (= :u.id (any (select [:id] (from :ActiveUsers)))))

Note, AZQL treat all forms in 'where' macro as SQL-functions, except 'select'.
So, you must use 'select' in you subqueries or pass them as a value. Example:

    :::clojure
    (let [sq (fields [:id] (table :ActiveUsers))]
      (select
        (from :u :Users)
        (where (= :u.id (any sq)))))

### Limit and offest

Library supports limiting of results (with offset):

    :::clojure
    (select
      (from :u :Users)
      (where (like? :name "%Andrei%"))
      (limit 100)
      (offset 25))

### CRUD

Library supports all CRUD operations.
Intert new records:

    :::clojure
    (insert! :table
      (values [{:field1 1, :field2 "value"}, {:field1 2, :field2 "value"}]))

Columns can be explicitly specified:

    :::clojure
    (insert! :table
      (fields [:a :b])
      (values [{:a 1, :b 2}, {:a 3, :b 4}]))

Update:

    :::clojure
    (update! :table
      (setf :cnt (+ :cnt 1))
      (setf :vale "new-value")
      (where (in? :id [1 2 3])))

Delete:

    :::clojure
    (delete! :table (where (= :id 1)))


### Custom dialects

TODO


### Raw queries

TODO


### License

Copyright © 2012 Andrei Zhlobich

Distributed under the Eclipse Public License, the same as Clojure.
