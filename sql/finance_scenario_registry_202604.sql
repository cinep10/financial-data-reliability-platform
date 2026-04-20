INSERT INTO scenario_experiment_run
(
    profile_id,
    scenario_name,
    scenario_type,
    dt_from,
    dt_to,
    parameters_json
)
VALUES
('finance_bank','baseline','unified','2026-04-01','2026-04-01','{"intensity":"medium"}'),
('finance_bank','baseline','unified','2026-04-02','2026-04-02','{"intensity":"medium"}'),
('finance_bank','baseline','unified','2026-04-03','2026-04-03','{"intensity":"medium"}'),

('finance_bank','partial_missing_auth','unified','2026-04-04','2026-04-04','{"intensity":"mild"}'),
('finance_bank','partial_missing_auth','unified','2026-04-05','2026-04-05','{"intensity":"medium"}'),
('finance_bank','partial_missing_card','unified','2026-04-06','2026-04-06','{"intensity":"medium"}'),
('finance_bank','partial_missing_loan','unified','2026-04-07','2026-04-07','{"intensity":"high"}'),

('finance_bank','duplicate_auth','unified','2026-04-08','2026-04-08','{"intensity":"mild"}'),
('finance_bank','duplicate_card','unified','2026-04-09','2026-04-09','{"intensity":"medium"}'),
('finance_bank','duplicate_loan','unified','2026-04-10','2026-04-10','{"intensity":"high"}'),

('finance_bank','delay_auth','unified','2026-04-11','2026-04-11','{"intensity":"mild"}'),
('finance_bank','delay_branch','unified','2026-04-12','2026-04-12','{"intensity":"medium"}'),
('finance_bank','delay_card','unified','2026-04-13','2026-04-13','{"intensity":"high"}'),

('finance_bank','partial_missing_auth','unified','2026-04-14','2026-04-14','{"intensity":"medium"}'),
('finance_bank','duplicate_card','unified','2026-04-15','2026-04-15','{"intensity":"medium"}'),
('finance_bank','delay_branch','unified','2026-04-16','2026-04-16','{"intensity":"medium"}'),

('finance_bank','baseline','unified','2026-04-17','2026-04-17','{"intensity":"medium"}'),
('finance_bank','partial_missing_card','unified','2026-04-18','2026-04-18','{"intensity":"mild"}'),
('finance_bank','duplicate_auth','unified','2026-04-19','2026-04-19','{"intensity":"medium"}'),
('finance_bank','delay_auth','unified','2026-04-20','2026-04-20','{"intensity":"medium"}'),

('finance_bank','baseline','unified','2026-04-21','2026-04-21','{"intensity":"medium"}'),
('finance_bank','partial_missing_loan','unified','2026-04-22','2026-04-22','{"intensity":"medium"}'),
('finance_bank','duplicate_loan','unified','2026-04-23','2026-04-23','{"intensity":"mild"}'),
('finance_bank','delay_card','unified','2026-04-24','2026-04-24','{"intensity":"medium"}'),

('finance_bank','baseline','unified','2026-04-25','2026-04-25','{"intensity":"medium"}'),
('finance_bank','partial_missing_auth','unified','2026-04-26','2026-04-26','{"intensity":"high"}'),
('finance_bank','duplicate_card','unified','2026-04-27','2026-04-27','{"intensity":"high"}'),
('finance_bank','delay_branch','unified','2026-04-28','2026-04-28','{"intensity":"high"}'),

('finance_bank','baseline','unified','2026-04-29','2026-04-29','{"intensity":"medium"}'),
('finance_bank','baseline','unified','2026-04-30','2026-04-30','{"intensity":"medium"}');
