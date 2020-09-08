select e.*,f.text as question_original,g.text as answer_original FROM
(SELECT
	c.id,
    SUBSTRING_INDEX( SUBSTRING_INDEX( c.asr_aid, ',', d.help_topic_id + 1 ), ',',- 1 ) AS asr_aids,
  c.sid,
  c.ylid,
  c.pid,
	c.asr_qids,
	c.question_result,
	c.question_zresult,
	c.answer_result,
	c.answer_zresult
FROM
(SELECT
    a.id,
    SUBSTRING_INDEX( SUBSTRING_INDEX( a.asr_qid, ',', b.help_topic_id + 1 ), ',',- 1 ) AS asr_qids,
  a.sid,
  a.ylid,
  a.pid,
	a.asr_aid,
	a.question_result,
	a.question_zresult,
	a.answer_result,
	a.answer_zresult
FROM
    `q_yt_lable_result_202007` AS a 
		JOIN mysql.help_topic AS b ON b.help_topic_id < ( length( a.asr_qid ) - length( REPLACE ( a.asr_qid, ',', '' ) ) + 1 )
		) AS c
		JOIN mysql.help_topic AS d ON d.help_topic_id < ( length( c.asr_aid ) - length( REPLACE ( c.asr_aid, ',', '' ) ) + 1 )
		) AS e
LEFT JOIN
(SELECT id,pid,text FROM
tcall_dgg_net.asr_list_202007) f
on e.asr_qids = f.id and e.pid = f.pid
LEFT JOIN
(SELECT id,pid,text FROM
tcall_dgg_net.asr_list_202007) g
on e.asr_aids = g.id and e.pid = g.pid;