
import { Registry } from "prom-client";
import { createBrokerMetrics, updateBrokerMetrics } from "./src/broker/metrics";

async function verify_metrics() {
    console.log("Verifying Broker Metrics...");
    const registry = new Registry();
    const metrics = createBrokerMetrics(registry);
    
    const stats: any = {
        pools: [
            {
                name: "test-pool",
                total: 10,
                idle: 5,
                in_use: 5,
                waiters: 0,
                acquires_total: 100,
                releases_total: 95,
                errors_total: 1,
                health_failures_total: 2,
                idle_timeouts_total: 3
            }
        ],
        scheduler: { pools: [] }
    };
    
    updateBrokerMetrics(metrics, stats);
    const output = await registry.metrics();
    
    console.log("Metrics output snippet:");
    console.log(output.split('\n').filter(line => line.includes('vertebra_broker_pool_health_failures_total') || line.includes('vertebra_broker_pool_idle_timeouts_total')).join('\n'));
    
    if (output.includes('vertebra_broker_pool_health_failures_total') && 
        output.includes('vertebra_broker_pool_idle_timeouts_total')) {
        console.log("\nPhase 4 verification passed!");
    } else {
        console.error("\nPhase 4 verification FAILED: Metrics not found");
        process.exit(1);
    }
}

verify_metrics();
