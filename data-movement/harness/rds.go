package harness

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatch"
	cwtypes "github.com/aws/aws-sdk-go-v2/service/cloudwatch/types"
	"github.com/aws/aws-sdk-go-v2/service/rds"
)

// RDSEndpoint ties a database connection to the Terraform-provided instance
// identifier used by the RDS and CloudWatch APIs.
type RDSEndpoint struct {
	Role       string
	Identifier string
	DB         *DB
}

// RDSControl uses the benchmark host's instance profile for cold starts and
// observability. It never accepts or persists AWS access keys.
type RDSControl struct {
	rds        *rds.Client
	cloudWatch *cloudwatch.Client
}

// NewRDSControl loads the normal AWS credential chain. On the benchmark host
// this resolves to refreshable credentials from IMDSv2.
func NewRDSControl(ctx context.Context) (*RDSControl, error) {
	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		return nil, fmt.Errorf("load AWS config: %w", err)
	}
	return &RDSControl{rds: rds.NewFromConfig(cfg), cloudWatch: cloudwatch.NewFromConfig(cfg)}, nil
}

// RDSEndpoints returns only ends backed by Terraform RDS instances. Local
// containers and a remote Iceberg sink have no corresponding identifier.
func RDSEndpoints(env *Env) []RDSEndpoint {
	var out []RDSEndpoint
	for _, end := range []struct {
		role string
		db   *DB
	}{{"source", env.Source}, {"sink", env.Sink}} {
		if end.db == nil || end.db.Engine == Iceberg {
			continue
		}
		name := fmt.Sprintf("BENCH_%s_%s_RDS_ID", strings.ToUpper(end.role), strings.ToUpper(end.db.Engine.Name()))
		if id := os.Getenv(name); id != "" {
			out = append(out, RDSEndpoint{Role: end.role, Identifier: id, DB: end.db})
		}
	}
	return out
}

// Reboot restarts all route endpoints concurrently and waits for both the RDS
// state and a real SQL connection to recover.
func (c *RDSControl) Reboot(ctx context.Context, endpoints []RDSEndpoint) (time.Duration, error) {
	started := time.Now()
	errs := make([]error, len(endpoints))
	var wg sync.WaitGroup
	for i, endpoint := range endpoints {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := c.rds.RebootDBInstance(ctx, &rds.RebootDBInstanceInput{
				DBInstanceIdentifier: aws.String(endpoint.Identifier),
			}); err != nil {
				errs[i] = fmt.Errorf("reboot %s %s: %w", endpoint.Role, endpoint.Identifier, err)
				return
			}
			waiter := rds.NewDBInstanceAvailableWaiter(c.rds)
			if err := waiter.Wait(ctx, &rds.DescribeDBInstancesInput{
				DBInstanceIdentifier: aws.String(endpoint.Identifier),
			}, 20*time.Minute); err != nil {
				errs[i] = fmt.Errorf("wait for %s %s: %w", endpoint.Role, endpoint.Identifier, err)
				return
			}
			if err := waitForSQL(ctx, endpoint.DB, 2*time.Minute); err != nil {
				errs[i] = fmt.Errorf("wait for %s SQL: %w", endpoint.Role, err)
			}
		}()
	}
	wg.Wait()
	if err := errors.Join(errs...); err != nil {
		return time.Since(started), err
	}
	return time.Since(started), nil
}

func waitForSQL(ctx context.Context, db *DB, timeout time.Duration) error {
	waitCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	h, err := db.Open()
	if err != nil {
		return err
	}
	defer func() { _ = h.Close() }()
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	var lastErr error
	for {
		if err := h.PingContext(waitCtx); err == nil {
			return nil
		} else {
			lastErr = err
		}
		select {
		case <-waitCtx.Done():
			return fmt.Errorf("SQL did not recover: %v: %w", lastErr, waitCtx.Err())
		case <-ticker.C:
		}
	}
}

var rdsMetricSpecs = []struct {
	name string
	stat string
}{
	{"CPUUtilization", "Average"},
	{"FreeableMemory", "Minimum"},
	{"DatabaseConnections", "Maximum"},
	{"DiskQueueDepth", "Maximum"},
	{"ReadIOPS", "Average"},
	{"WriteIOPS", "Average"},
	{"ReadLatency", "Average"},
	{"WriteLatency", "Average"},
	{"ReadThroughput", "Average"},
	{"WriteThroughput", "Average"},
	{"NetworkReceiveThroughput", "Average"},
	{"NetworkTransmitThroughput", "Average"},
	{"FreeStorageSpace", "Minimum"},
	{"OldestReplicationSlotLag", "Maximum"},
	{"TransactionLogsDiskUsage", "Maximum"},
}

// Metrics retrieves raw one-minute RDS points around the timed window. The
// padding retains boundary buckets rather than pretending a partial minute is
// an exact run aggregate.
func (c *RDSControl) Metrics(ctx context.Context, identifier string, started, ended time.Time) ([]MetricSeries, error) {
	queries := make([]cwtypes.MetricDataQuery, 0, len(rdsMetricSpecs))
	series := make(map[string]*MetricSeries, len(rdsMetricSpecs))
	for i, spec := range rdsMetricSpecs {
		id := fmt.Sprintf("m%d", i)
		queries = append(queries, cwtypes.MetricDataQuery{
			Id:         aws.String(id),
			Label:      aws.String(spec.name),
			ReturnData: aws.Bool(true),
			MetricStat: &cwtypes.MetricStat{
				Metric: &cwtypes.Metric{
					Namespace:  aws.String("AWS/RDS"),
					MetricName: aws.String(spec.name),
					Dimensions: []cwtypes.Dimension{{
						Name: aws.String("DBInstanceIdentifier"), Value: aws.String(identifier),
					}},
				},
				Period: aws.Int32(60),
				Stat:   aws.String(spec.stat),
			},
		})
		series[id] = &MetricSeries{Name: spec.name, Stat: spec.stat}
	}

	input := &cloudwatch.GetMetricDataInput{
		MetricDataQueries: queries,
		StartTime:         aws.Time(started.Add(-time.Minute)),
		EndTime:           aws.Time(ended.Add(time.Minute)),
		ScanBy:            cwtypes.ScanByTimestampAscending,
	}
	for {
		out, err := c.cloudWatch.GetMetricData(ctx, input)
		if err != nil {
			return nil, err
		}
		for _, result := range out.MetricDataResults {
			dst := series[aws.ToString(result.Id)]
			if dst == nil {
				continue
			}
			for i := 0; i < len(result.Timestamps) && i < len(result.Values); i++ {
				dst.Points = append(dst.Points, MetricPoint{At: result.Timestamps[i], Value: result.Values[i]})
			}
		}
		if out.NextToken == nil || aws.ToString(out.NextToken) == "" {
			break
		}
		input.NextToken = out.NextToken
	}

	result := make([]MetricSeries, 0, len(rdsMetricSpecs))
	for i := range rdsMetricSpecs {
		item := series[fmt.Sprintf("m%d", i)]
		sort.Slice(item.Points, func(i, j int) bool { return item.Points[i].At.Before(item.Points[j].At) })
		result = append(result, *item)
	}
	return result, nil
}
