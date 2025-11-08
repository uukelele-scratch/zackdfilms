from vocabulary import word_index
import random, threading, os, subprocess as sp, numpy as np
from hybridoma import App, portal
from moviepy.editor import VideoFileClip, concatenate_videoclips
from PIL import Image, ImageDraw, ImageFont
from moviepy.config import get_setting

app = App(__name__)
CHANNEL_NAME = "Zack D. Films"

@portal.expose
async def create_video(sentence):
    sentence = sentence.strip().lower().split()
    clips = []

    for w in sentence:
        if w not in word_index:
            # e = json.dumps({'error': 'We couldn\'t find the word: ' + w, 'word':w})
            e = {'error': 'We couldn\'t find the word: ' + w, 'word':w}
            # yield f"event: error\ndata: {e}\n\n"
            await portal.log(event='error', data=e)
            return
        sel = random.choice(word_index[w])
        # yield f"event: progress\ndata: {json.dumps({'step':'loaded', 'word': w})}\n\n"
        await portal.log(event='progress', data={'step': 'loaded', 'word': w})
        clips.append(VideoFileClip(sel["video_path"]).subclip(sel["start"], sel["end"]))

    # yield f"event: progress\ndata: {json.dumps({'step':'concatenating'})}\n\n"
    await portal.log(event='progress', data={'step': 'concatenating'})
    final = concatenate_videoclips(clips, method="compose")
    w, h = final.size
    fps = getattr(final, "fps", 24)

    # yield f"event: progress\ndata: {json.dumps({'step':'rendering'})}\n\n"
    await portal.log(event='progress', data={'step': 'rendering'})
    cmd = [
        get_setting("FFMPEG_BINARY"),
        "-y",

        "-f", "rawvideo",
        "-pix_fmt", "rgb24",
        "-s", f"{w}x{h}",
        "-r", str(fps),
        "-i", "pipe:0"
    ]
    audio_pipe_read_fd = -1
    audio_pipe_write_fd = -1

    audio_fps = 44100
    audio_channels = 2
    audio_format = "s16le"

    audio_pipe_read_fd, audio_pipe_write_fd = os.pipe()

    cmd.extend([
        "-f", audio_format,
        "-ar", str(audio_fps),
        "-ac", str(audio_channels),
        "-i", f"pipe:{audio_pipe_read_fd}",
    ])

    cmd.extend([
        "-map", "0:v:0",
        "-map", "1:a:0",
    ])

    cmd.extend(["-c:a", "aac", "-b:a", "128k"])
    
    cmd.extend([
        "-c:v", "libx264",
        "-preset", "medium",
        "-tune", "fastdecode",
        "-pix_fmt", "yuv420p",
        "-movflags", "frag_keyframe+empty_moov+faststart",
        "-f", "mp4",
        "pipe:1",
    ])

    pass_fds = [audio_pipe_read_fd]
    proc = sp.Popen(
        cmd,
        stdin=sp.PIPE,
        stdout=sp.PIPE,
        stderr=sp.PIPE,
        pass_fds=pass_fds,
    )

    video_thread = None
    audio_thread = None
    writer_error = None

    def write_video_data():
        nonlocal writer_error
        try:
            buffer_size = 10 * 1024 * 1024
            current_buffer = b''
            total_frames = int(final.duration * fps) if final.duration else 0

            if total_frames <= 0:
                print("Warning: Video duration or FPS is zero or invalid. No frames to write.")
                raise ValueError("Cannot process video with zero duration or fps.")

            font = ImageFont.truetype("assets/font.ttf", size=20)
            interval_frames = int(2.5 * fps)
            x, y = random.randint(0, final.w - 200), random.randint(0, final.h - 50)

            for i in range(total_frames):
                t = i / fps
                frame_np = final.get_frame(t)
                frame_bytes = frame_np.tobytes()

                if i % interval_frames == 0:
                    x, y = random.randint(0, final.w - 200), random.randint(0, final.h - 50)

                frame_img = Image.fromarray(frame_np)
                draw = ImageDraw.Draw(frame_img)
                draw.text((x, y), "zdf.mce.run", font=font, fill=(255, 255, 255, 6))
                frame_np = np.array(frame_img)

                frame_bytes = frame_np.tobytes()
                current_buffer += frame_bytes
                if len(current_buffer) >= buffer_size:
                    if proc.stdin and not proc.stdin.closed:
                        proc.stdin.write(current_buffer)
                    else:
                        print("Video writer: stdin closed prematurely. Stopping.")
                        break
                    current_buffer = b''

            if current_buffer and proc.stdin and not proc.stdin.closed:
                proc.stdin.write(current_buffer)

        except Exception as e:
            print(f"ERROR in video writer thread: {e}")
            import traceback
            traceback.print_exc()
            writer_error = e
        finally:
            if proc.stdin and not proc.stdin.closed:
                try:
                    proc.stdin.close()
                except OSError as oe:
                    print(f"Video writer: Warning - error closing stdin: {oe}")
            else:
                print("Video writer: stdin already closed before finalization.")

    def write_audio_data():
        nonlocal writer_error
        audio_pipe_write_stream = os.fdopen(audio_pipe_write_fd, 'wb')
        try:
            chunksize = 4096
            samples_written = 0

            for chunk in final.audio.iter_chunks(chunksize=chunksize, fps=audio_fps, quantize=True, nbytes=2, logger=None):
                audio_pipe_write_stream.write(chunk)
                samples_written += len(chunk) // (audio_channels * 2)
        except Exception as e:
            print(f"ERROR in audio writer thread: {e}")
            writer_error = e
        finally:
            if audio_pipe_write_stream:
                audio_pipe_write_stream.close()
            elif audio_pipe_write_fd != -1:
                 try:
                    os.close(audio_pipe_write_fd)
                 except OSError:
                    pass


    video_thread = threading.Thread(target=write_video_data)
    video_thread.start()

    if audio_pipe_read_fd != -1:
        os.close(audio_pipe_read_fd)
        audio_pipe_read_fd = -1

    audio_thread = threading.Thread(target=write_audio_data)
    audio_thread.start()

    video_thread.join()
    audio_thread.join()

    video_bytes = proc.stdout.read()
    err_bytes = proc.stderr.read()
    return_code = proc.wait()

    if proc.stdout: proc.stdout.close()
    if proc.stderr: proc.stderr.close()

    if writer_error:
        # yield f"event: error\ndata: {json.dumps({'msg':'Data writing error','detail':str(writer_error)})}\n\n"
        await portal.log(event="error", data={'msg': 'Data writing error', 'detail': str(writer_error)})
        return
    
    err_str = err_bytes.decode(errors='ignore') # Decode stderr

    if return_code != 0:
        # yield f"event: error\ndata: {json.dumps({'msg':'FFmpeg execution error','detail':err_str})}\n\n"
        await portal.log(event="error", data={'msg': 'FFmpeg execution error', 'detail': err_str})
        print("FFmpeg Error Output:\n", err_str)
        return

    # b64 = json.dumps({"video_base64": base64.b64encode(video_bytes).decode("ascii")})
    # yield f"event: done\ndata: {b64}\n\n"
    await portal.log(event="done", data={"video_bytes": video_bytes})

    # return video_bytes
    return None


@app.route("/")
def index():
    return app.render("index.html")

if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=9979)
