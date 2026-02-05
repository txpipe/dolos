
CREATE TABLE public.ada_pots (
	id bigserial NOT NULL,
	slot_no public."word63type" NOT NULL,
	epoch_no public."word31type" NOT NULL,
	treasury public."lovelace" NOT NULL,
	reserves public."lovelace" NOT NULL,
	rewards public."lovelace" NOT NULL,
	utxo public."lovelace" NOT NULL,
	deposits_stake public."lovelace" NOT NULL,
	fees public."lovelace" NOT NULL,
	block_id int8 NOT NULL,
	deposits_drep public."lovelace" NOT NULL,
	deposits_proposal public."lovelace" NOT NULL,
	CONSTRAINT ada_pots_pkey PRIMARY KEY (id)
);
CREATE UNIQUE INDEX unique_ada_pots ON public.ada_pots USING btree (block_id);


-- public.epoch definition

-- Drop table

-- DROP TABLE public.epoch;

CREATE TABLE public.epoch (
	id bigserial NOT NULL,
	out_sum public."word128type" NOT NULL,
	fees public."lovelace" NOT NULL,
	tx_count public."word31type" NOT NULL,
	blk_count public."word31type" NOT NULL,
	"no" public."word31type" NOT NULL,
	start_time timestamp NOT NULL,
	end_time timestamp NOT NULL,
	CONSTRAINT epoch_pkey PRIMARY KEY (id),
	CONSTRAINT unique_epoch UNIQUE (no)
);
CREATE INDEX idx_epoch_no ON public.epoch USING btree (no);


CREATE TABLE public.epoch_param (
	id bigserial NOT NULL,
	epoch_no public."word31type" NOT NULL,
	min_fee_a public."word31type" NOT NULL,
	min_fee_b public."word31type" NOT NULL,
	max_block_size public."word31type" NOT NULL,
	max_tx_size public."word31type" NOT NULL,
	max_bh_size public."word31type" NOT NULL,
	key_deposit public."lovelace" NOT NULL,
	pool_deposit public."lovelace" NOT NULL,
	max_epoch public."word31type" NOT NULL,
	optimal_pool_count public."word31type" NOT NULL,
	influence float8 NOT NULL,
	monetary_expand_rate float8 NOT NULL,
	treasury_growth_rate float8 NOT NULL,
	decentralisation float8 NOT NULL,
	protocol_major public."word31type" NOT NULL,
	protocol_minor public."word31type" NOT NULL,
	min_utxo_value public."lovelace" NOT NULL,
	min_pool_cost public."lovelace" NOT NULL,
	nonce public.hash32type NULL,
	cost_model_id int8 NULL,
	price_mem float8 NULL,
	price_step float8 NULL,
	max_tx_ex_mem public."word64type" NULL,
	max_tx_ex_steps public."word64type" NULL,
	max_block_ex_mem public."word64type" NULL,
	max_block_ex_steps public."word64type" NULL,
	max_val_size public."word64type" NULL,
	collateral_percent public."word31type" NULL,
	max_collateral_inputs public."word31type" NULL,
	block_id int8 NOT NULL,
	extra_entropy public.hash32type NULL,
	coins_per_utxo_size public."lovelace" NULL,
	pvt_motion_no_confidence float8 NULL,
	pvt_committee_normal float8 NULL,
	pvt_committee_no_confidence float8 NULL,
	pvt_hard_fork_initiation float8 NULL,
	dvt_motion_no_confidence float8 NULL,
	dvt_committee_normal float8 NULL,
	dvt_committee_no_confidence float8 NULL,
	dvt_update_to_constitution float8 NULL,
	dvt_hard_fork_initiation float8 NULL,
	dvt_p_p_network_group float8 NULL,
	dvt_p_p_economic_group float8 NULL,
	dvt_p_p_technical_group float8 NULL,
	dvt_p_p_gov_group float8 NULL,
	dvt_treasury_withdrawal float8 NULL,
	committee_min_size public."word64type" NULL,
	committee_max_term_length public."word64type" NULL,
	gov_action_lifetime public."word64type" NULL,
	gov_action_deposit public."word64type" NULL,
	drep_deposit public."word64type" NULL,
	drep_activity public."word64type" NULL,
	pvtpp_security_group float8 NULL,
	min_fee_ref_script_cost_per_byte float8 NULL,
	CONSTRAINT epoch_param_pkey PRIMARY KEY (id)
);
CREATE INDEX idx_epoch_param_block_id ON public.epoch_param USING btree (block_id);
CREATE INDEX idx_epoch_param_cost_model_id ON public.epoch_param USING btree (cost_model_id);
CREATE UNIQUE INDEX unique_epoch_param ON public.epoch_param USING btree (epoch_no, block_id);

CREATE TABLE public.epoch_stake (
	id bigserial NOT NULL,
	addr_id int8 NOT NULL,
	pool_id int8 NOT NULL,
	amount public."lovelace" NOT NULL,
	epoch_no public."word31type" NOT NULL,
	CONSTRAINT epoch_stake_pkey PRIMARY KEY (id),
	CONSTRAINT unique_epoch_stake UNIQUE (epoch_no, addr_id, pool_id)
);
CREATE UNIQUE INDEX bf_u_idx_epoch_stake_epoch_and_id ON public.epoch_stake USING btree (epoch_no, id);
CREATE INDEX idx_epoch_stake_addr_id ON public.epoch_stake USING btree (addr_id);
CREATE INDEX idx_epoch_stake_epoch_no ON public.epoch_stake USING btree (epoch_no);
CREATE INDEX idx_epoch_stake_pool_id ON public.epoch_stake USING btree (pool_id);
CREATE UNIQUE INDEX unique_stake ON public.epoch_stake USING btree (epoch_no, addr_id, pool_id);

CREATE TABLE public.pool_hash (
	id bigserial NOT NULL,
	hash_raw public.hash28type NOT NULL,
	"view" varchar NOT NULL,
	CONSTRAINT pool_hash_pkey PRIMARY KEY (id),
	CONSTRAINT unique_pool_hash UNIQUE (hash_raw)
);
CREATE INDEX bf_idx_pool_hash_view ON public.pool_hash USING hash (view);


CREATE TABLE public.pool_owner (
	id bigserial NOT NULL,
	addr_id int8 NOT NULL,
	pool_update_id int8 NOT NULL,
	CONSTRAINT pool_owner_pkey PRIMARY KEY (id)
);
CREATE INDEX pool_owner_pool_update_id_idx ON public.pool_owner USING btree (pool_update_id);
CREATE UNIQUE INDEX unique_pool_owner ON public.pool_owner USING btree (addr_id, pool_update_id);

CREATE TABLE public.pool_retire (
	id bigserial NOT NULL,
	hash_id int8 NOT NULL,
	cert_index int4 NOT NULL,
	announced_tx_id int8 NOT NULL,
	retiring_epoch public."word31type" NOT NULL,
	CONSTRAINT pool_retire_pkey PRIMARY KEY (id)
);
CREATE INDEX idx_pool_retire_announced_tx_id ON public.pool_retire USING btree (announced_tx_id);
CREATE INDEX idx_pool_retire_hash_id ON public.pool_retire USING btree (hash_id);
CREATE UNIQUE INDEX unique_pool_retiring ON public.pool_retire USING btree (announced_tx_id, cert_index);

-- public.pool_update definition

-- Drop table

-- DROP TABLE public.pool_update;

CREATE TABLE public.pool_update (
	id bigserial NOT NULL,
	hash_id int8 NOT NULL,
	cert_index int4 NOT NULL,
	vrf_key_hash public.hash32type NOT NULL,
	pledge public."lovelace" NOT NULL,
	active_epoch_no int8 NOT NULL,
	meta_id int8 NULL,
	margin float8 NOT NULL,
	fixed_cost public."lovelace" NOT NULL,
	registered_tx_id int8 NOT NULL,
	reward_addr_id int8 NOT NULL,
	deposit public."lovelace" NULL,
	CONSTRAINT pool_update_pkey PRIMARY KEY (id)
);
CREATE INDEX idx_pool_update_active_epoch_no ON public.pool_update USING btree (active_epoch_no);
CREATE INDEX idx_pool_update_hash_id ON public.pool_update USING btree (hash_id);
CREATE INDEX idx_pool_update_meta_id ON public.pool_update USING btree (meta_id);
CREATE INDEX idx_pool_update_registered_tx_id ON public.pool_update USING btree (registered_tx_id);
CREATE INDEX idx_pool_update_reward_addr ON public.pool_update USING btree (reward_addr_id);
CREATE UNIQUE INDEX unique_pool_update ON public.pool_update USING btree (registered_tx_id, cert_index);

-- public.reward definition

-- Drop table

-- DROP TABLE public.reward;

CREATE TABLE public.reward (
	addr_id int8 NOT NULL,
	"type" public."rewardtype" NOT NULL,
	amount public."lovelace" NOT NULL,
	spendable_epoch int8 NOT NULL,
	pool_id int8 NOT NULL,
	earned_epoch int8 GENERATED ALWAYS AS (
CASE
    WHEN type = 'refund'::rewardtype THEN spendable_epoch
    ELSE
    CASE
        WHEN spendable_epoch >= 2 THEN spendable_epoch - 2
        ELSE 0::bigint
    END
END) STORED NOT NULL,
	CONSTRAINT unique_reward UNIQUE (addr_id, type, earned_epoch, pool_id)
);
CREATE INDEX idx_reward_addr_id ON public.reward USING btree (addr_id);
CREATE INDEX idx_reward_earned_epoch ON public.reward USING btree (earned_epoch);
CREATE INDEX idx_reward_pool_id ON public.reward USING btree (pool_id);
CREATE INDEX idx_reward_spendable_epoch ON public.reward USING btree (spendable_epoch);

-- public.stake_address definition

-- Drop table

-- DROP TABLE public.stake_address;

CREATE TABLE public.stake_address (
	id bigserial NOT NULL,
	hash_raw public.addr29type NOT NULL,
	"view" varchar NOT NULL,
	script_hash public.hash28type NULL,
	CONSTRAINT stake_address_pkey PRIMARY KEY (id),
	CONSTRAINT unique_stake_address UNIQUE (hash_raw)
);
CREATE INDEX idx_stake_address_hash_raw ON public.stake_address USING btree (hash_raw);
CREATE INDEX idx_stake_address_view ON public.stake_address USING hash (view);
CREATE INDEX stake_address_idx ON public.stake_address USING btree (view);

CREATE TABLE public.stake_deregistration (
	id bigserial NOT NULL,
	addr_id int8 NOT NULL,
	cert_index int4 NOT NULL,
	epoch_no public."word31type" NOT NULL,
	tx_id int8 NOT NULL,
	redeemer_id int8 NULL,
	CONSTRAINT stake_deregistration_pkey PRIMARY KEY (id)
);
CREATE INDEX idx_stake_deregistration_addr_id ON public.stake_deregistration USING btree (addr_id);
CREATE INDEX idx_stake_deregistration_redeemer_id ON public.stake_deregistration USING btree (redeemer_id);
CREATE INDEX idx_stake_deregistration_tx_id ON public.stake_deregistration USING btree (tx_id);
CREATE UNIQUE INDEX unique_stake_deregistration ON public.stake_deregistration USING btree (tx_id, cert_index);

CREATE TABLE public.stake_registration (
	id bigserial NOT NULL,
	addr_id int8 NOT NULL,
	cert_index int4 NOT NULL,
	epoch_no public."word31type" NOT NULL,
	tx_id int8 NOT NULL,
	deposit public."lovelace" NULL,
	CONSTRAINT stake_registration_pkey PRIMARY KEY (id)
);
CREATE INDEX idx_stake_registration_addr_id ON public.stake_registration USING btree (addr_id);
CREATE INDEX idx_stake_registration_tx_id ON public.stake_registration USING btree (tx_id);
CREATE UNIQUE INDEX unique_stake_registration ON public.stake_registration USING btree (tx_id, cert_index);
