<?php
$con = new PDO("mysql:host=localhost;dbname=childes", "root", "88560848");
if (count($argv) < 2) {
    print("Please, inform a corpus.\n");
    die();
}

$corpus_name = $argv[1];

$dirname = "./corpora/" . $corpus_name;

if (!is_dir($dirname)) {
    print("Corpus files not found.\n");
    die();
}

$punctuation_mapping = [
    'q' => '?',
    'e' => '!',
    'trail off'=>'.',
    'p'=> '.',
    'quotation next line' => ':',
    'interruption'=>'.',
    'trail off question'=>'?',
    'self interruption'=> '.',
    'quotation precedes'=>':',
    'self interruption question'=>'?',
    'interruption question'=>'?'
];

$rs = $con->query("SELECT * FROM corpus WHERE name = '$corpus_name'");

function insert($table, $data)
{
    global $con;
    $keys = array_keys($data);
    $refs = array_map(function($value) { return ':'.$value; }, $keys);
    $sql = "INSERT INTO $table (" . implode(",", $keys) . ") VALUES (" . implode(',', $refs) . ")";

    
    try {
        $stmt = $con->prepare($sql)->execute($data);
        print("SaADASDSDA\n");
        return $con->lastInsertId();
    } catch (PDOExecption $e) {
        //throw $th;
        print($e->getMessage());
        print("\n");
        print($sql);
        print("\n");
        // var_dump($data);
        print("\n");
        print("\n");
        //throw $th;
    }

}

function select($sql, $args, $all = true)
{
    global $con;
    try {
        $sth = $con->prepare($sql);
        $sth->execute($args);
        if ($all) {
            return $sth->fetchAll(PDO::FETCH_ASSOC);
        }
        return $sth->fetch(PDO::FETCH_ASSOC);
    } catch (PDOExecption $e) {
        //throw $th;
        print($e->getMessage());
        print("\n");
        print($sql);
        print("\n");
        // var_dump($args);
        print("\n");
        print("\n");
    }

}

function get_participante($id){
    global $con;
    $rs = $con->query("SELECT * FROM participant WHERE id = {$id}");
    return $rs->fetch(PDO::FETCH_ASSOC); 
}

function get_child_with_tag($node, $tag){
    foreach($node->children() as $child){
        // print($child->getName());
        if($child->getName() == $tag){
            return $child;
        }else{
            $c = get_child_with_tag($child, $tag);
            if($c != null){
                return $c;
            }
        }
    }
    return null;
}

$corpus = $rs->fetch(PDO::FETCH_ASSOC);
$directory = new \RecursiveDirectoryIterator($dirname);
$iterator = new \RecursiveIteratorIterator($directory);

foreach ($iterator as $info) {
    if ($info->isFile()) {
        $file_name = $info->getPathname();

        $transcript_test = $con->query("SELECT * FROM transcript_jap
            WHERE corpus_id = {$corpus['id']} and filename = '$file_name'");

        $transcript = $transcript_test->fetch(PDO::FETCH_ASSOC);

        if ($transcript == null) {
            $transcript = [
                'filename' => $file_name,
                'corpus_id' => $corpus['id'],
                'finished' => 0,
            ];

            $transcript['id'] = insert("transcript_jap", $transcript);
        }

        if ($transcript['finished'] == 0) {
            $xml = simplexml_load_file($file_name);
            $participants = [];
            foreach ($xml->Participants->participant as $participant) {
                if ($participant['id'] == "CHI") {
                    $ps = select("SELECT * FROM participant
                        WHERE corpus_id = ? AND name = ? AND code = ? AND role = ?", [$corpus['id'], $participant['name'], $participant['id'], $participant['role']], false);

                    if ($ps != null) {
                        $participants["CHI"] = $ps['id'];
                    }
                }

            }

            foreach ($xml->Participants->participant as $participant) {
                if (array_key_exists('CHI', $participants)) {

                    $sql = "SELECT * FROM participant
                    WHERE corpus_id = ?  AND code = ? AND role = ? AND target_child_id = ?";
                    $coditions = [$corpus['id'], $participant['id'], $participant['role'], $participants['CHI']];
                    
                    if(isset($participant['name'])){
                        $sql .= " AND name = ?";
                        $coditions[]  = $participant['name'];
                    }
                    
                    $ps = select($sql, $coditions, false);
                    if ($ps != null) {
                        $participants[trim($participant['id'])] = $ps['id'];
                    }else{
                        // var_dump([$corpus['id'], $participant['name'], $participant['id'], $participant['role'], $participants['CHI']]);
                    }
                }
            }
            foreach($xml->u as $u){
                // var_dump($participants);
                $utterance = [
                    "speaker_id"=> $participants[trim($u['who'])],
                    'order' => preg_replace("/[^0-9\.]/", '', $u['uID']),
                    'corpus_id' => $corpus['id'],
                    'transcript_id' => $transcript['id'],
                    'length' => 0
                ];
                print("FILE: ".$file_name."\n");
                print("Utterance: ".$utterance['order']."\n");

                $speaker = get_participante($utterance['speaker_id']);

                if(isset($speaker['target_child_id'])){
                    $target_child = get_participante($speaker['target_child_id']);

                    $utterance['target_child_id'] = $speaker['target_child_id'];
                    $utterance['target_child_age'] = $target_child['min_age'];
                    $utterance['target_child_name'] = $target_child['name'];
                    $utterance['target_child_sex'] = $target_child['sex'];

                    // $utterance['target_child_id'] = $speaker['target_child_id']
                }

                $ut = select("SELECT id, length FROM utterance_jap 
                    WHERE speaker_id = ? AND `order` = ? AND corpus_id = ? AND transcript_id = ?",
                    [$utterance['speaker_id'], $utterance['order'], $corpus['id'],$transcript['id']], false);
                
                if($ut != null){
                    $utterance = $ut;
                }else{
                    $utterance['id'] = insert("utterance_jap", $utterance);

                }
                // var_dump($utterance);
                $i = 0;
                foreach($u->children() as $token){
                    $tk = [
                        "speaker_id" => $speaker['id'],
                        "utterance_id" => $utterance['id'],
                        "token_order" => $i,
                        "corpus_id" => $corpus['id'],
                        'stem' => '',
                        'gloss' => ''
                    ];
                    // var_dump($tk);
                    $test_tk = select("SELECT * FROM token_jap WHERE utterance_id = ? AND token_order = ?", [$tk['utterance_id'],$tk['token_order']]);
                    // var_dump($test_tk);

                    if($test_tk == null && in_array($token->getName(),['g','w','tagMarker','t'])){
                        if($token->getName() == 'g'){
                            $w = get_child_with_tag($token,'w');
                            if($w != null){
                                $token = $w;
                            }
                        }

                        // word
                        if($token->getName() == 'w'){
                            $tk['gloss'] = $token;
                            if(isset($token->replacement)){
                                $tk['replacement'] = $token->replacement;
                                $token = $token->replacement;
                            }

                            // morphology
                            if(isset($token->mor)){
                                if(isset($token->mor->mwc)){
                                    $tk['part_of_speech'] = $token->mor->mwc->pos->c;
                                    foreach($token->mor->mwc->mw as $mw){
                                        $tk['stem'] .= $mw->stem;
                                    }
                                }else{
                                    $tk['part_of_speech'] = $token->mor->mw->pos->c;
                                    $tk['stem'] = $token->mor->mw->stem;
                                }
                                // gra.attrib['index']+"|"+gra.attrib['head']+"|"+gra.attrib['relation']
                                if(isset($token->gra)){
                                    $tk['relation'] = $token->gra['index']."|".$token->gra['head']."|".$token->gra['relation'];
                                }
                            }

                            if(isset($token->shortening)){
                                if($tk['stem'] == ''){
                                    $tk['stem'] = $tk['gloss'];
                                }else{
                                    $tk['gloss']  = $tk['stem'];
                                }
                            }

                            if(!isset($tk['gloss']) || empty($tk['gloss'])){
                                $tk['gloss']  = $tk['stem'];
                            }

                        }

                        // punctuation
                        if($token->getName() == 't'){
                            if(array_key_exists(trim($token['type']), $punctuation_mapping)){
                                $tk['gloss'] = $punctuation_mapping[trim($token['type'])];
                                $tk['stem'] = $punctuation_mapping[trim($token['type'])];
                                $tk['part_of_speech'] = "f";
                                
                                if(isset($token->mor)){
                                    if(isset($token->mor->gra)){
                                        $tk['relation'] = $token->mor->gra['index']."|".$token->mor->gra['head']."|".$token->mor->gra['relation'];
                                    }
                                }

                                
                            }
                        }

                        // comma
                        if(in_array($token->getName(), ['tagMarker'])){
                            if(array_key_exists($token['type'], $punctuation_mapping)){
                                $tk['gloss'] = ",";
                                $tk['stem'] = ",";
                                $tk['part_of_speech'] = "f";
                                
                                if(isset($token->mor)){
                                    if(isset($token->mor->gra)){
                                        $tk['relation'] = $token->mor->gra['index']."|".$token->mor->gra['head']."|".$token->mor->gra['relation'];
                                    }
                                }

                                
                            }
                        }
                        $i++;
                        print("\t".$tk['gloss']."\n");
                        insert('token_jap',$tk);
                    }
                    // 
                }

                if($i >0){
                    $utterance['length'] += $i;
                    $con->prepare("UPDATE utterance_jap SET length = ? WHERE id = ?")->execute([$utterance['length'],$utterance['id']]);

                }

            }

            $con->prepare("UPDATE transcript_jap SET finished = 1 WHERE id = ?")->execute([$transcript['id']]);
        }

        //
    }
}
