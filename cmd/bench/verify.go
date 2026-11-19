package main

import (
	"errors"
	"fmt"
)

// verify proves each finished job's result in SQL. It is only valid when
// nothing else wrote to the job's deals while it ran (true in the kill phase,
// which runs without interactive load).
func (b *bench) verify(rows []jobRow) error {
	fmt.Println("  proof:")
	allOK := true
	for _, j := range rows {
		var notInTarget, transitions, duplicated, wrongVersion int
		err := b.pool.QueryRow(b.ctx, `
			SELECT
			  -- deals in the snapshot that are not in the target stage
			  (SELECT count(*) FROM bulk_job_items i JOIN opportunities o ON o.id = i.opportunity_id
			   WHERE i.job_id = j.id AND o.stage_id <> j.target_stage_id),
			  -- transitions this job recorded
			  (SELECT count(*) FROM stage_transitions WHERE bulk_job_id = j.id),
			  -- deals with more than one transition from this job
			  (SELECT count(*) FROM (SELECT 1 FROM stage_transitions WHERE bulk_job_id = j.id
			                         GROUP BY opportunity_id HAVING count(*) > 1) d),
			  -- deals whose version is not snapshot+1 (moved) or snapshot (skipped)
			  (SELECT count(*) FROM bulk_job_items i
			   JOIN opportunities o ON o.id = i.opportunity_id
			   LEFT JOIN stage_transitions t ON t.bulk_job_id = i.job_id AND t.opportunity_id = i.opportunity_id
			   WHERE i.job_id = j.id
			     AND o.version <> i.snapshot_version + CASE WHEN t.id IS NULL THEN 0 ELSE 1 END)
			FROM bulk_jobs j WHERE j.id = $1`, j.id).Scan(&notInTarget, &transitions, &duplicated, &wrongVersion)
		if err != nil {
			return fmt.Errorf("verify job %d: %w", j.id, err)
		}

		fmt.Printf("    job %d (workspace %d, claimed %d×):\n", j.id, j.ws, j.claims)
		checks := []struct {
			ok  bool
			msg string
		}{
			{j.status == "completed" && j.cursor == j.total,
				fmt.Sprintf("status %s, cursor %d/%d", j.status, j.cursor, j.total)},
			{j.moved+j.skipped == j.total,
				fmt.Sprintf("moved %d + skipped %d = total %d", j.moved, j.skipped, j.total)},
			{notInTarget == 0,
				fmt.Sprintf("every snapshot deal is in the target stage (%d are not)", notInTarget)},
			{transitions == j.moved,
				fmt.Sprintf("%d transitions recorded = %d moved", transitions, j.moved)},
			{duplicated == 0,
				fmt.Sprintf("no deal moved twice (%d were)", duplicated)},
			{wrongVersion == 0,
				fmt.Sprintf("each deal written at most once: version = snapshot+1 if moved, unchanged if skipped (%d differ)", wrongVersion)},
		}
		for _, c := range checks {
			mark := "PASS"
			if !c.ok {
				mark, allOK = "FAIL", false
			}
			fmt.Printf("      [%s] %s\n", mark, c.msg)
		}
	}
	if !allOK {
		return errors.New("kill and resume: result is NOT correct (see FAIL lines)")
	}
	return nil
}
