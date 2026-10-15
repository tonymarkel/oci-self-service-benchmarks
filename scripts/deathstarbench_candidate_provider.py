"""Cloud wiring for private support-stage operators, never public dispatch."""

from __future__ import annotations

import copy

from app import main as application
from app.providers import aws, azure, gcp, oci
from app.run_lease import require_acquired_run_lease
from scripts import qualify_aws_oci_deathstarbench_distributed as legacy_pins
from scripts.deathstarbench_candidate_operator_state import canonical, fingerprint


CLEANUP_SOURCE_KEY = 'candidate_cloud_cleanup_source'


class CandidateCloud:
    def __init__(self, plan, provider_pin, *, persist, emit, oci_config_file=None):
        self.plan = plan
        self.pin = copy.deepcopy(provider_pin)
        self.persist, self.emit = persist, emit
        self.oci_config_file = oci_config_file
        self._clients = None

    def _guard(self, job, lease):
        require_acquired_run_lease(lease, job['id'])
        saved = self.plan.model_dump(exclude={'ssh_private_key', 'ssh_public_key', 'ssh_key_passphrase'})
        if canonical(saved) != canonical(job['plan']):
            raise ValueError('Infrastructure plan changed at the provider boundary.')
        if canonical(self.pin) != canonical(job['candidate_support_pin']['provider_pin']):
            raise ValueError('Provider selector changed at the provider boundary.')
        if self.plan.provider in ('azure', 'gcp'):
            from scripts.deathstarbench_candidate_cloud_audit import require_known_candidate_resource_keys
            require_known_candidate_resource_keys(job)

    def clients(self):
        if self.plan.provider == 'oci' and self._clients is None:
            from argparse import Namespace
            self._clients = legacy_pins._oci_clients(
                Namespace(oci_config_file=self.oci_config_file, oci_profile=self.pin['profile']), self.pin,
            )
        return self._clients

    def provision(self, job, lease):
        self._guard(job, lease)
        common = {'persist': self.persist, 'emit': self.emit}
        if self.plan.provider == 'aws':
            aws.provision_distributed_deathstarbench_candidate(
                job, self.plan, public_key=job['_public_key'],
                support_image=self.pin['support_image'], application_image=self.pin['application_image'], **common,
            )
            aws.validate_distributed_deathstarbench_candidate(job, persist=self.persist)
        elif self.plan.provider == 'oci':
            oci.provision_distributed_deathstarbench_candidate(
                job, inputs=legacy_pins._oci_inputs(self.pin, {'public_key': job['_public_key']}),
                clients=self.clients(), **common,
            )
            oci.publish_distributed_runtime_projection(job, clients=self.clients(), persist=self.persist)
        else:
            provider = azure if self.plan.provider == 'azure' else gcp
            provider.provision_distributed_deathstarbench_candidate(
                job, self.plan, public_key=job['_public_key'], **common,
            )
        self._guard(job, lease)

    def audit(self, job, lease):
        self._guard(job, lease)
        from scripts import deathstarbench_candidate_cloud_audit as audit
        if self.plan.provider == 'azure':
            return audit.audit_azure_ready(job, self.plan)
        if self.plan.provider == 'gcp':
            return audit.audit_gcp_ready(job, self.plan)
        if self.plan.provider == 'aws':
            # Existing AWS SDK audit also publishes local aliases. Isolate that
            # publication and require the real persisted aliases already match.
            probe = {**job, 'resources': copy.deepcopy(job['resources'])}
            projection = aws.validate_distributed_deathstarbench_candidate(probe)
            if canonical(probe['resources']) != canonical(job['resources']):
                raise ValueError('AWS audit would adopt or repair persisted identities/aliases.')
            state = aws._dsb_load(probe)
            identity = {
                'run_id': job['id'], 'provider': 'aws', 'account_id': state['contract']['account_id'],
                'contract': state['contract'], 'projection': projection,
                'resource_ids': {key: entry['id'] for key, entry in state['graph'].items()},
            }
        else:
            contract = oci.validate_distributed_deathstarbench_candidate(job, clients=self.clients(), require_ready=True)
            projection = oci.distributed_runtime_projection(job, clients=self.clients())
            identity = {
                'run_id': job['id'], 'provider': 'oci', 'inputs': contract['inputs'],
                'projection': projection,
                'resource_ids': {key: entry['id'] for key, entry in contract['entries'].items()},
            }
        self._guard(job, lease)
        return {
            'schema_version': 1, 'candidate_schema': 'candidate-support-cloud-ready-v1',
            'candidate_only': True, 'read_only': True, 'run_id': job['id'],
            'provider': self.plan.provider, 'cloud_identity': identity,
            'cloud_identity_sha256': fingerprint(identity),
        }

    def reconcile_aws_capacity_rejection(self, job, lease, *, event_id):
        """Explicit cleanup-only bridge; never part of default destroy/resume."""
        self._guard(job, lease)
        if self.plan.provider != 'aws':
            raise ValueError('Capacity-rejection reconciliation is AWS-only.')
        result = aws.reconcile_distributed_capacity_rejection_for_cleanup(
            job, lease=lease, event_id=event_id, persist=self.persist,
        )
        self._guard(job, lease)
        return result

    def destroy_and_prove(self, job, lease):
        self._guard(job, lease)
        from scripts import deathstarbench_candidate_cloud_audit as audit
        source = job.get(CLEANUP_SOURCE_KEY)
        if source is None:
            value = {
                'schema_version': 1, 'candidate_schema': 'candidate-support-cleanup-source-v1',
                'run_id': job['id'], 'provider': self.plan.provider,
                'pin_fingerprint': job['candidate_support_pin']['fingerprint'],
                'plan_sha256': fingerprint(job['plan']), 'resources': copy.deepcopy(job['resources']),
                'cloud_identity': copy.deepcopy(job.get('candidate_support_operator', {}).get('cloud_identity')),
            }
            source = {**value, 'fingerprint': fingerprint(value)}
            job[CLEANUP_SOURCE_KEY] = copy.deepcopy(source)
            # Provider cleanup may durably clear its entire graph. Keep its
            # exact original GET targets before the first delete, so a crash
            # after graph clearing cannot strand independent absence checks.
            self.persist(job)
            self._guard(job, lease)
            if canonical(job.get(CLEANUP_SOURCE_KEY)) != canonical(source):
                raise ValueError('Cleanup source changed during durable publication.')
        required = {'schema_version', 'candidate_schema', 'run_id', 'provider', 'pin_fingerprint',
                    'plan_sha256', 'resources', 'cloud_identity', 'fingerprint'}
        if (not isinstance(source, dict) or set(source) != required
                or type(source['schema_version']) is not int or source['schema_version'] != 1
                or source['candidate_schema'] != 'candidate-support-cleanup-source-v1'
                or source['run_id'] != job['id'] or source['provider'] != self.plan.provider
                or source['pin_fingerprint'] != job['candidate_support_pin']['fingerprint']
                or source['plan_sha256'] != fingerprint(job['plan'])
                or not isinstance(source['resources'], dict)
                or source['fingerprint'] != fingerprint({key: source[key] for key in source if key != 'fingerprint'})):
            raise ValueError('Retained cleanup source differs from its exact run/provider pin.')
        original = {**job, 'resources': copy.deepcopy(source['resources'])}
        common = {'persist': self.persist, 'emit': self.emit}
        provider = self.plan.provider
        if provider == 'aws':
            aws.destroy_distributed_deathstarbench_candidate(job, **common)
            aws.recover_distributed_deathstarbench_candidate(job, persist=self.persist, wait_for_attachments=False)
            complete = aws.distributed_deathstarbench_candidate_deleted(job)
            absence = {'retained_tombstones_reconciled': True}
        elif provider == 'oci':
            oci.destroy_distributed_deathstarbench_candidate(job, clients=self.clients(), **common)
            oci.attest_distributed_candidate_terminal_deletion(job, clients=self.clients())
            complete = oci.distributed_candidate_is_deleted(job)
            absence = {'retained_tombstones_reconciled': True}
        else:
            if provider == 'azure':
                expected = source['cloud_identity']
                if expected is not None:
                    audit.require_azure_cleanup_identity(original, expected)
                azure.destroy_resources(job, **common)
                absence = audit.audit_azure_absent(original)
            else:
                gcp.destroy_resources(job, **common)
                absence = audit.audit_gcp_absent(original)
            complete = application._cleared_distributed_provider_contract(job, provider)
        self._guard(job, lease)
        if not complete:
            raise ValueError('Provider cleanup lacks its strict terminal deletion contract.')
        return {
            'schema_version': 1, 'candidate_schema': 'candidate-support-cleanup-proof-v1',
            'candidate_only': True, 'run_id': job['id'], 'provider': provider,
            'cleanup_complete': True, 'independent_absence_proved': True,
            'provider_absence': absence,
        }
