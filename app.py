from flask import Flask, g
from flask import request, jsonify, make_response
import io
import wave
import hashlib
import db
from scipy.io import wavfile

app = Flask(__name__)
db.init_db(app)

@app.teardown_appcontext
def close_connection(ex):
    db = getattr(g, '_database', None)
    if db is not None:
        db.close()

@app.route("/post", methods=['POST'])
def upload_files():
    if len(list(request.files.keys())) > 1:
        response = {}
        for name, file in request.files.items():
            content = file.read()
            content_digest = hashlib.md5(content).hexdigest()
            try:
                # get info about the wav file
                in_mem = io.BytesIO(content)
                with wave.open(in_mem, 'rb') as wav_file:
                    (nchannels, _, framerate, nframes, comptype, _) = wav_file.getparams()
                    duration = nframes / framerate
                # check if file already exists in database with the same name and digest
                res = db.query_db("SELECT COUNT(*) AS COUNT FROM file_info WHERE name=? and contentHash=?", (name, content_digest), one=True)
                # upload the file to database if it doesn't exist
                if res['COUNT'] == 0:
                    db.commit_db("INSERT INTO file_info (name, contentHash, channels, framerate, frames, duration, comptype) VALUES (?,?,?,?,?,?,?)", (name, content_digest, nchannels, framerate, nframes, duration, comptype))
                    db.commit_db("INSERT INTO file_store (contentHash, content) VALUES (?,?)", (content_digest, content))
                    response[name] = {'message': f'successfully uploaded {name}.', 'code': 201}
                else:
                    response[name] = {'message': f'file {name} is already uploaded.', 'code': 200}
            except:
                response['file'] = {'message': 'Bad Request, provide a valid .wav file.', 'code': 400}
        return make_response(jsonify(response), 400)
    else:
        response = {'message': 'Bad Request, provide a formdata with {filename1: file1, ...} as the data.', 'code': 400}
        return make_response(jsonify(response), 400)


"""/chunk?name=<file>&start=<frame>&end=<frame>"""
@app.route("/chunk", methods=['GET'])
def download_chunk():
    name = request.args['name']
    start = int(request.args['start'])
    end = int(request.args['end'])
    # check existence
    query = 'SELECT * FROM file_chunks where name=? AND start=? AND end=?'
    args = (name, start, end)
    row = db.query_db(query, args, one=True)
    if row is not None:
        app.logger.debug(f'found {args}')
        return {'name': row['name'], 'start': start, 'end': end, 'data': row['content'], 'code': 200}

    # otherwise, retieve the parent audio file
    query, args = download_helper(request)
    row = db.query_db(query, args, one=True)

    in_mem = io.BytesIO(row['content'])
    rate, wav_arr = wavfile.read(in_mem)

    chunked = io.BytesIO()
    wavfile.write(chunked, rate, wav_arr[start:end+1])
    chunked_file_str = str(chunked.read())

    # inser the new chunk, if necessary
    query = 'INSERT INTO file_chunks (name, start, end, content) VALUES (?,?,?,?)'
    args = (name, start, end, chunked_file_str)
    db.commit_db(query, args)

    return {'name': row['name'], 'data': chunked_file_str, 'start': start, 'end': end, 'code': 200}


@app.route("/download", methods=['GET'])
def download_files():
    query, args = download_helper(request)
    files = []
    for row in db.query_db(query, args):
        files.append({'name': row['name'], 'data': str(row['content']), 'code': 200})
    if len(files) > 0:
        return {'files': files, 'code': 200}
    else:
        response = {'message': 'Bad Request, file does not exist.', 'code': 400}
        return make_response(jsonify(response), 400)

@app.route("/info", methods=['GET'])
def info_files():
    query, args = info_helper(request)
    files = []
    for row in db.query_db(query, args):
        row_dict = {k: row[k] for k in row.keys()}
        row_dict['status'] = 200
        files.append(row_dict)
    if len(files) > 0:
        return {'files': files, 'code': 200}
    else:
        response = {'message': 'Bad Request, file(s) may not exist in server.', 'code': 400}
        return make_response(jsonify(response), 400)

@app.route("/list", methods=['GET'])
def list_files():
    query, args = list_helper(request)
    files = []
    for row in db.query_db(query, args):
        files.append(row['name'])
    return {'files': files, 'code': 200}


def download_helper(request):
    name = request.args.get('name')
    query = ("SELECT * FROM (SELECT * FROM file_info WHERE name=?) AS a INNER JOIN file_store ON a.contentHash=file_store.contentHash" 
    if name is not None else "SELECT * FROM file_info INNER JOIN file_store ON file_info.contentHash=file_store.contentHash")
    return query, (name,) if name is not None else ()

def info_helper(request):
    name = request.args.get('name')
    query = ("SELECT name, channels, framerate, frames, duration, comptype FROM (SELECT * FROM file_info WHERE name=?) AS a INNER JOIN file_store ON a.contentHash=file_store.contentHash" 
    if name is not None else "SELECT name, channels, framerate, frames, duration, comptype FROM file_info INNER JOIN file_store ON file_info.contentHash=file_store.contentHash")
    return query, (name,) if name is not None else ()

def list_helper(request):
    allowed_args = {
        'channels': 'channels=?',
        'maxduration': 'duration<=?',
        'minduration': 'duration>=?',
        'maxframerate': 'framerate<=?',
        'minframerate': 'framerate>=?',
        'maxframes': 'frames<=?', 
        'minframes': 'frames>=?'}

    query = "SELECT name FROM file_info"
    queries = list(request.args.keys())
    if len(queries) > 0:
        for idx, key in enumerate(queries):
            if key not in allowed_args:
                response = {'message': f'invalid query param {key}.', 'code': 400}
                return make_response(jsonify(response), 400)
            else:
                query = query + (' WHERE' if idx == 0 else ' AND') + f' {allowed_args[key]}'
    return query, [float(v) for v in request.args.values()]