package main

import (
	"bytes"
	"context"
	_ "embed"
	"flag"
	"io"
	"log/slog"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"text/template"
	"time"

	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/image"
	"github.com/docker/docker/client"
	"github.com/docker/go-connections/nat"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/pkg/errors"
	"github.com/pressly/goose/v3"
	"github.com/rinchsan/gosimports"
	"golang.org/x/sync/errgroup"
	"golang.org/x/text/cases"
	"golang.org/x/text/language"
)

var (
	migrationPath = flag.String(
		"src",
		".db/postgres/migrations",
		"where are the migration files stored",
	)
	dst = flag.String("dst", "internal/store/postgres/tables", "where to store the table structures")

	imageName = flag.String("image", "postgres:alpine", "name of the docker image with postgres")

	rawInitialisms = flag.String("initialisms", "", "comma separated initialisms")

	logger = slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{AddSource: true}))
)

func main() {
	flag.Parse()

	for _, s := range strings.Split(*rawInitialisms, ",") {
		initialisms[s] = struct{}{}
	}

	ctx := context.Background()
	cli, err := client.NewClientWithOpts()
	if err != nil {
		panic(err)
	}

	creds := ("tablegen_" + uuid.NewString())[:20]

	db, containerID, err := SetUpContainer(ctx, cli, creds)
	if err != nil {
		panic(err)
	}

	defer RemoveContainer(ctx, cli, containerID)

	err = RunMigrations(ctx, db, *migrationPath)
	if err != nil {
		panic(err)
	}

	tables, err := GetTables(ctx, db)
	if err != nil {
		panic(err)
	}

	primaryKeys, err := GetPrimaryKeys(ctx, db)
	if err != nil {
		panic(err)
	}

	data, err := CreateFileData(tables, primaryKeys)
	if err != nil {
		panic(err)
	}

	err = WriteFiles(*dst, data)
	if err != nil {
		panic(err)
	}
}

func SetUpContainer(ctx context.Context, cli *client.Client, creds string) (db *pgxpool.Pool, containerID string, err error) {
	defer func() {
		if containerID != "" && err != nil {
			RemoveContainer(ctx, cli, containerID)
		}
	}()

	out, err := cli.ImagePull(ctx, *imageName, image.PullOptions{})
	if err != nil {
		return nil, "", errors.WithStack(err)
	}

	defer func() {
		if err := out.Close(); err != nil {
			logger.Error("closing read closer", slog.Any("err", err))
		}
	}()

	_, err = io.Copy(os.Stdout, out)
	if err != nil {
		logger.Error("copying reader", slog.Any("err", err))
	}

	resp, err := cli.ContainerCreate(
		ctx,
		&container.Config{
			Image: *imageName,
			Cmd: []string{
				"postgres",
				"-c", "fsync=off",
				"-c", "shared_buffers=2048MB",
				"-c", "max_locks_per_transaction=1024",
				"-c", "synchronous_commit=off",
				"-c", "fsync=false",
				"-c", "full_page_writes=false",
				"-c", "log_statement=none",
				"-c", "max_wal_size=5GB",
				"-c", "maintenance_work_mem=2GB",
				"-c", "wal_level=minimal",
				"-c", "archive_mode=off",
				"-c", "max_wal_senders=0",
			},
			Env: []string{
				"POSTGRES_DB=" + creds,
				"POSTGRES_USER=" + creds,
				"POSTGRES_PASSWORD=" + creds,
			},
			ExposedPorts: nat.PortSet{
				"5432/tcp": struct{}{},
			},
			// TODO: Figure out why it doesn't work.
			// Healthcheck: &container.HealthConfig{
			// 	Test:     []string{"/usr/bin/pg_isready", "-h", "localhost", "-p", "5432"},
			// 	Interval: 100 * time.Millisecond,
			// 	Timeout:  10 * time.Second,
			// },
		},
		&container.HostConfig{
			PortBindings: nat.PortMap{
				"5432/tcp": []nat.PortBinding{
					{
						HostIP:   "localhost",
						HostPort: "0",
					},
				},
			},
		},
		nil,
		nil,
		creds,
	)
	if err != nil {
		return nil, "", errors.WithStack(err)
	}

	err = cli.ContainerStart(ctx, resp.ID, container.StartOptions{})
	if err != nil {
		return nil, resp.ID, errors.WithStack(err)
	}

	logs, err := cli.ContainerLogs(
		ctx,
		resp.ID,
		container.LogsOptions{ShowStdout: true, ShowStderr: true, Timestamps: true},
	)
	if err != nil {
		logger.Error("getting logs", slog.Any("err", err))
	}

	defer func() {
		if err := logs.Close(); err != nil {
			logger.Error("closing read closer", slog.Any("err", err))
		}
	}()

	_, err = io.Copy(os.Stdout, logs)
	if err != nil {
		logger.Error("copying reader", slog.Any("err", err))
	}

	info, err := cli.ContainerInspect(ctx, resp.ID)
	if err != nil {
		return nil, "", errors.WithStack(err)
	}

	port := info.NetworkSettings.Ports["5432/tcp"][0].HostPort

	dsn := url.URL{
		Scheme: "postgres",
		User:   url.UserPassword(creds, creds),
		Host:   net.JoinHostPort("localhost", port),
		Path:   creds,
	}

	db, err = pgxpool.New(ctx, dsn.String())
	if err != nil {
		return nil, "", errors.WithStack(err)
	}

	start := time.Now()
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for range ticker.C {
		err = db.Ping(ctx)
		if err == nil {
			break
		}

		if time.Since(start) > 10*time.Second {
			return nil, "", errors.Wrapf(err, "connecting to the db")
		}
	}

	return db, resp.ID, nil
}

func RemoveContainer(ctx context.Context, cli *client.Client, containerID string) {
	err := cli.ContainerStop(ctx, containerID, container.StopOptions{})
	if err != nil {
		logger.Error("stopping container", slog.Any("err", err))
	}

	err = cli.ContainerRemove(ctx, containerID, container.RemoveOptions{})
	if err != nil {
		logger.Error("removing container", slog.Any("err", err))
	}
}

func RunMigrations(ctx context.Context, db *pgxpool.Pool, path string) error {
	err := goose.RunContext(ctx, "up", stdlib.OpenDBFromPool(db), path)
	return errors.WithStack(err)
}

func GetTables(ctx context.Context, db *pgxpool.Pool) (map[Table][]Column, error) {
	q := `
	SELECT
	    table_schema,
	    table_name,
	    column_name,
	    column_default,
	    is_nullable::BOOL,
	    udt_name,
	    character_maximum_length
	FROM information_schema.columns
	WHERE table_schema NOT IN ('information_schema', 'pg_catalog')
	ORDER BY table_name, LENGTH(column_name), column_name;`

	rows, err := db.Query(ctx, q)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	defer rows.Close()

	var t Table
	var c Column
	m := map[Table][]Column{}
	for rows.Next() {
		err = rows.Scan(&t.Schema, &t.Name, &c.Name, &c.Default, &c.Nullable, &c.Type, &c.CharLength)
		if err != nil {
			return nil, errors.WithStack(err)
		}

		m[t] = append(m[t], c)
	}

	return m, errors.WithStack(rows.Err())
}

func GetPrimaryKeys(ctx context.Context, db *pgxpool.Pool) (map[Table][]string, error) {
	q := `
	SELECT i.indrelid::REGCLASS, a.attname
	FROM pg_index i
	JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY (i.indkey)
	WHERE i.indisprimary
	ORDER BY i.indrelid, LENGTH(a.attname), a.attname;`

	rows, err := db.Query(ctx, q)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	defer rows.Close()

	var t Table
	var c string
	m := map[Table][]string{}
	for rows.Next() {
		err = rows.Scan(&t.Name, &c)
		if err != nil {
			return nil, errors.WithStack(err)
		}

		t.Schema = "public"
		if strings.Contains(t.Name, ".") {
			schemaAndName := strings.SplitN(t.Name, ".", 2)
			t.Schema, t.Name = schemaAndName[0], schemaAndName[1]
		}

		m[t] = append(m[t], c)
	}

	return m, errors.WithStack(rows.Err())
}

//go:embed templates/main_file.go
var mainFile []byte

//go:embed templates/tables.gohtml
var tablesTemplate string

var templateFuncs = template.FuncMap{
	"snakeToCamelCase": snakeToCamelCase,
	"selectColumnType": SelectColumnType,
	"insertColumnType": InsertColumnType,
	"updateColumnType": UpdateColumnType,
	"columnType":       ColumnType,
}

func CreateFileData(tables map[Table][]Column, primaryKeys map[Table][]string) (map[string][]byte, error) {
	fileName := "tables.go"
	b, err := gosimports.Process(fileName, mainFile, nil)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	files := make(map[string][]byte, len(tables)+1)
	files[fileName] = b

	templ, err := template.New("").Funcs(templateFuncs).Parse(tablesTemplate)
	if err != nil {
		return files, errors.WithStack(err)
	}

	var buf bytes.Buffer
	for table, columns := range tables {
		snakelike := strings.Replace(table.GetName(), ".", "_", 1)
		fileName = snakelike + ".go"
		if _, exist := files[fileName]; exist {
			for i := 2; true; i++ {
				fileName = snakelike + strconv.Itoa(i) + ".go"
				if _, exist = files[fileName]; !exist {
					break
				}
			}
		}

		buf.Reset()
		err = table.Write(templ, &buf, primaryKeys[table], columns)
		if err != nil {
			return files, err
		}

		b = append([]byte{}, buf.Bytes()...)
		b, err = gosimports.Process(fileName, b, nil)
		if err != nil {
			return files, errors.Wrapf(err, "formatting %q: %s", fileName, buf.Bytes())
		}

		files[fileName] = b
	}

	return files, nil
}

func WriteFiles(dst string, files map[string][]byte) error {
	err := os.RemoveAll(dst)
	if err != nil {
		return errors.Wrapf(err, "removing %q", dst)
	}

	err = os.MkdirAll(dst, 0750)
	if err != nil {
		return errors.Wrapf(err, "creating %q", dst)
	}

	var g errgroup.Group
	g.SetLimit(20)

	for fileName, data := range files {
		fileName, data := fileName, data
		g.Go(func() error {
			return writeFile(filepath.Join(dst, fileName), data)
		})
	}

	return g.Wait()
}

func writeFile(fileName string, data []byte) error {
	file, err := os.Create(fileName)
	if err != nil {
		return errors.Wrapf(err, "creating %q", fileName)
	}

	defer file.Close()

	_, err = file.Write(data)
	return errors.Wrapf(err, "writing to %q", fileName)
}

type Table struct{ Schema, Name string }

func (t Table) GetName() string {
	if t.Schema == "public" {
		return t.Name
	}
	return t.Schema + "." + t.Name
}

func (t Table) Write(templ *template.Template, buf *bytes.Buffer, primaryKeys []string, cols []Column) error {
	if len(cols) == 0 {
		return nil
	}

	primary := make(map[string]struct{}, len(primaryKeys))
	for _, key := range primaryKeys {
		primary[key] = struct{}{}
	}

	for i := range cols {
		_, cols[i].IsPrimaryKey = primary[cols[i].Name]
	}

	tableName := t.GetName()
	camelCasedTableName := snakeToCamelCase(tableName)

	err := templ.Execute(
		buf,
		map[string]any{
			"CamelCasedTableName": camelCasedTableName,
			"TableType":           camelCasedTableName + "Table",
			"TableColsType":       camelCasedTableName + "TableCols",
			"TableName":           tableName,
			"Cols":                cols,
			"PrimaryKeys":         primaryKeys,
		},
	)
	return errors.Wrapf(err, "executing template for %q", tableName)
}

type Column struct {
	Name         string
	Type         string
	Default      *string
	CharLength   *int
	Nullable     bool
	IsPrimaryKey bool
}

func ColumnType(c Column, isGetter ...any) string {
	switch c.Type {
	case "bytea":
		return "[]byte"
	case "bool":
		return "bool"
	case "int8":
		return "int64"
	case "int2":
		return "int16"
	case "int4":
		return "int32"
	case "text", "varchar", "uuid":
		return "string"
	case "json", "jsonb":
		if len(isGetter) > 0 {
			return "json.RawMessage"
		}
		return "any"
	case "float4":
		return "float32"
	case "float8":
		return "float64"
	case "date":
		return "civil.Date"
	case "time", "timetz":
		return "civil.Time"
	case "timestamp", "timestamptz":
		return "time.Time"
	case "numeric":
		return "decimal.Decimal"
	case "interval":
		return "time.Duration"
	default:
		logger.With(slog.String("type", c.Type)).Error("unknown type")
		return "any"
	}
}

func SelectColumnType(c Column) (s string) {
	if c.Nullable {
		switch c.Type {
		case "json", "jsonb", "bytea":
		default:
			s += "*"
		}
	}

	s += ColumnType(c, true)

	s += " // " + c.Type
	if c.CharLength != nil {
		s += "(" + strconv.Itoa(*c.CharLength) + ")"
	}
	if c.Nullable {
		s += " null"
	}
	if c.Default != nil {
		s += " default(" + *c.Default + ")"
	}

	return s
}

func InsertColumnType(c Column) (s string) {
	if c.Nullable || c.Default != nil {
		switch c.Type {
		case "json", "jsonb", "bytea":
		default:
			s += "*"
		}
	}

	s += ColumnType(c)

	s += " // " + c.Type
	if c.CharLength != nil {
		s += "(" + strconv.Itoa(*c.CharLength) + ")"
	}
	if c.Nullable {
		s += " null"
	}
	if c.Default != nil {
		s += " default(" + *c.Default + ")"
	}

	return s
}

func UpdateColumnType(c Column) (s string) {
	if c.Nullable || c.Default != nil {
		switch c.Type {
		case "json", "jsonb":
		default:
			s += "*"
		}
	} else {
		switch c.Type {
		case "bytea", "json", "jsonb":
		default:
			s += "*"
		}
	}

	s += ColumnType(c)

	s += " // " + c.Type
	if c.CharLength != nil {
		s += "(" + strconv.Itoa(*c.CharLength) + ")"
	}
	if c.Nullable {
		s += " null"
	}
	if c.Default != nil {
		s += " default(" + *c.Default + ")"
	}

	return s
}

var (
	upperCase   = cases.Title(language.English)
	initialisms = map[string]struct{}{"id": {}, "url": {}, "db": {}}
)

func snakeToCamelCase(s string) string {
	var b strings.Builder
	b.Grow(len(s))

	for _, s := range strings.FieldsFunc(s, func(r rune) bool { return r == '_' || r == '.' }) {
		if _, ok := initialisms[s]; ok {
			b.WriteString(strings.ToUpper(s))
		} else {
			b.WriteString(upperCase.String(s))
		}
	}

	return b.String()
}
