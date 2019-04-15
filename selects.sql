select * from token_jap;

select id, name, role, corpus_id, min_age/365, min_age/30 from participant 
where role = "Target_Child" 
and min_age/30 between 28 and 36
order by min_age asc;

select * from utterance where speaker_id = 2464;

select * from transcript_by_speaker;

select avg(mlu) as mean_mlu, speaker_id from transcript_by_speaker 
group by speaker_id order by mean_mlu;

select id, target_child_name, target_child_age/30 months, mlu, speaker_id 
from transcript_by_speaker where mlu <> 0 and speaker_role = "Target_Child";

select * from participant where id = 323;

select avg(A.mlu) mlu_avg, A.speaker_id , B.name, B.min_age/30 months, B.corpus_id
from transcript_by_speaker A
inner join participant B ON B.id = A.speaker_id
where A.speaker_role = "Target_Child" 
AND B.min_age/30 between 30 and 144
group by A.speaker_id  having mlu_avg >= 2.25  order by avg(A.mlu) desc;

select A.corpus_id, B.name, count(*) from transcript_by_speaker A
inner join corpus B ON A.corpus_id =B.id
where A.speaker_role = 'Target_Child' and A.mlu > 2.25
and A.target_child_age/30 between 30 and 144 group by A.corpus_id order by B.name;


select B.* from transcript_by_speaker A
inner join transcript B ON A.transcript_id = B.id
where A.speaker_role = 'Target_Child' and A.mlu > 2.25
and A.target_child_age/30 between 30 and 144;


set global max_prepared_stmt_count = 100000;

