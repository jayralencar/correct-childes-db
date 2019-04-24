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

$rs = $con->query("SELECT * FROM corpus WHERE name = '$corpus_name'");

function insert($table, $data)
{
    global $con;
    $keys = array_keys($data);
    
    $sql = "INSERT INTO $table (" . implode(",", $keys) . ") VALUES (" . implode(',', array_fill(0, count($keys), '?')) . ")";

    
    try {
        $stmt = $con->prepare($sql)->execute($data);
        return $con->lastInsertId();
    } catch (PDOExecption $e) {
        //throw $th;
        print($e->getMessage());
        print("\n");
        print($sql);
        print("\n");
        var_dump($data);
        print("\n");
        print("\n");
        //throw $th;
    }

}

function select($sql, $args, $all = true)
{
    // print($sql." - ".count($args)."\n");
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
        var_dump($args);
        print("\n");
        print("\n");
    }

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
                    $ps = select("SELECT * FROM participant
                        WHERE corpus_id = ? AND name = ? AND code = ? AND role = ? AND target_child_id = ?", [$corpus['id'], $participant['name'], $participant['id'], $participant['role'], $participants['CHI']], false);
                    if ($ps != null) {
                        $participants[trim($participant['id'])] = $ps['id'];
                    }
                }
            }
            var_dump($participants);
            print("\n");
            // foreach($xml->u as $u){
            //     $utterance = [
            //         "speaker_id"=> $participants[$u['who']],
            //         'order' => preg_replace("/[^0-9\.]/", '', $u['uID']),
            //         'corpus_id' => $corpus['id']
            //     ];
            //     // var_dump($utterance);

            // }
        }

        //
    }
}
