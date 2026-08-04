import type {ReactNode} from 'react';
import clsx from 'clsx';
import Link from '@docusaurus/Link';
import Layout from '@theme/Layout';
import Heading from '@theme/Heading';
import styles from './index.module.css';

type RouteCardProps = {
  eyebrow: string;
  title: string;
  description: string;
  to: string;
};

function RouteCard({eyebrow, title, description, to}: RouteCardProps): ReactNode {
  return (
    <Link className={styles.routeCard} to={to}>
      <span className={styles.routeEyebrow}>{eyebrow}</span>
      <Heading as="h3">{title}</Heading>
      <p>{description}</p>
      <span className={styles.routeAction}>Open guide <span aria-hidden>→</span></span>
    </Link>
  );
}

function Home(): ReactNode {
  return (
    <Layout
      title="Author, operate, and integrate Tickr"
      description="Release-matched documentation for Tickr workflow authors, data-plane operators, and API integrators."
    >
      <main>
        <section className={styles.hero}>
          <div className={styles.heroCopy}>
            <div className={styles.releaseLabel}>
              <span className={styles.liveDot} aria-hidden />
              Release line 0.1 · supported
            </div>
            <Heading as="h1">Keep every run<br />on time.</Heading>
            <p className={styles.lede}>
              Build workflows in Nickel, operate a formation with clear durability
              boundaries, and follow every asynchronous transition from registration
              to a terminal Run.
            </p>
            <div className={styles.actions}>
              <Link className="button button--primary button--lg" to="/docs/get-started">
                Start with Tickr Lite
              </Link>
              <Link className="button button--outline button--lg" to="/docs/concepts/execution-lifecycle">
                Understand execution
              </Link>
            </div>
          </div>
          <div className={styles.clockPanel} aria-label="First run sequence">
            <div className={styles.clockMark} aria-hidden>
              <svg viewBox="0 0 160 160" fill="none">
                <circle cx="80" cy="80" r="62" className={styles.clockFace} />
                {Array.from({length: 12}, (_, index) => {
                  const angle = (index * Math.PI) / 6;
                  const x1 = 80 + Math.sin(angle) * 52;
                  const y1 = 80 - Math.cos(angle) * 52;
                  const x2 = 80 + Math.sin(angle) * 59;
                  const y2 = 80 - Math.cos(angle) * 59;
                  return <line key={index} x1={x1} y1={y1} x2={x2} y2={y2} />;
                })}
                <path d="M80 80V34M80 80L111 61" className={styles.clockHands} />
                <circle cx="80" cy="80" r="4" className={styles.clockPivot} />
              </svg>
            </div>
            <ol className={styles.timeline}>
              <li><span>Access</span><strong>Obtain operator values</strong></li>
              <li><span>Local</span><strong>Start Tickr Lite</strong></li>
              <li><span>Build</span><strong>Register a workflow</strong></li>
              <li className={styles.timelineLive}><span>Run</span><strong>Inspect the outcome</strong></li>
            </ol>
          </div>
        </section>

        <div className={styles.timeRail} aria-hidden><span /></div>

        <section className={styles.routeSection}>
          <div className={styles.sectionIntro}>
            <span className={styles.sectionLabel}>Choose your route</span>
            <Heading as="h2">One system, four working views.</Heading>
            <p>Start from the outcome you own. The same vocabulary and release contract carry across every guide.</p>
          </div>
          <div className={styles.routeGrid}>
            <RouteCard
              eyebrow="01 · Evaluate"
              title="Run Tickr Lite"
              description="Connect an invited local data plane and take Hello from source to a completed Run."
              to="/docs/get-started"
            />
            <RouteCard
              eyebrow="02 · Author"
              title="Shape a workflow"
              description="Use the Core DSL to define Tasks, composition, gates, routing, and runtime Patches."
              to="/docs/author"
            />
            <RouteCard
              eyebrow="03 · Operate"
              title="Own the formation"
              description="Choose a supported profile and understand its storage, readiness, and failure boundaries."
              to="/docs/operate"
            />
            <RouteCard
              eyebrow="04 · Integrate"
              title="Drive the API"
              description="Work with asynchronous Commands, Signals, Runs, logs, Events, and public contracts."
              to="/docs/integrate"
            />
          </div>
        </section>

        <section className={styles.formationSection}>
          <div>
            <span className={styles.sectionLabel}>Formation map</span>
            <Heading as="h2">Lite first. Distributed when the topology demands it.</Heading>
          </div>
          <div className={styles.formationGrid}>
            <div className={clsx(styles.formation, styles.formationPrimary)}>
              <span>Primary path</span>
              <strong>lite-local</strong>
              <p>One process · SQLite · local files · one Executor</p>
            </div>
            <div className={styles.formation}>
              <span>Advanced</span>
              <strong>all-nats</strong>
              <p>Distributed · Postgres · JetStream · object storage</p>
            </div>
            <div className={styles.formation}>
              <span>Advanced</span>
              <strong>all-redis</strong>
              <p>Distributed · Postgres · Redis protocols · object storage</p>
            </div>
          </div>
        </section>
      </main>
    </Layout>
  );
}

export default Home;
