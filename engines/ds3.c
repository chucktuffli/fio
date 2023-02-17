/**
 * FIO engine for DAOS S3 (ds3).
 *
 * (C) Copyright 2022 Hewlett Packard Enterprise.
 */

#include <fio.h>
#include <optgroup.h>

#include <daos.h>
#include <daos_s3.h>

static bool		ds3_initialized;
static int		num_threads;
static pthread_mutex_t	daos_mutex = PTHREAD_MUTEX_INITIALIZER;
static ds3_t		*ds3 = NULL;
static ds3_bucket_t	*bhdl = NULL;

struct daos_iou {
	struct io_u	*io_u;
	daos_event_t	ev;
	daos_size_t	size;
	bool		complete;
};

struct daos_data {
	daos_handle_t	eqh;
	ds3_obj_t	*obj;
	struct io_u	**io_us;
	int		queued;
	int		num_ios;
};

struct ds3_fio_options {
	void		*pad;
	char		*pool;   /* Pool UUID */
	char		*bucket; /* Bucket UUID */
};

static struct fio_option options[] = {
	{
		.name		= "pool",
		.lname		= "pool uuid or label",
		.type		= FIO_OPT_STR_STORE,
		.off1		= offsetof(struct ds3_fio_options, pool),
		.help		= "DAOS pool uuid or label",
		.category	= FIO_OPT_C_ENGINE,
		.group		= FIO_OPT_G_DS3,
	},
	{
		.name           = "bucket",
		.lname          = "bucket uuid or label",
		.type           = FIO_OPT_STR_STORE,
		.off1           = offsetof(struct ds3_fio_options, bucket),
		.help           = "DAOS bucket uuid or label",
		.category	= FIO_OPT_C_ENGINE,
		.group		= FIO_OPT_G_DS3,
	},
	{
		.name           = NULL,
	},
};

static int ds3_fio_global_init(struct thread_data *td)
{
	struct ds3_fio_options	*eo = td->eo;
	int			rc = 0;

	if (!eo->pool || !eo->bucket) {
		log_err("Missing required DAOS options\n");
		return EINVAL;
	}

	rc = ds3_init();
	if (rc != -DER_ALREADY && rc) {
		log_err("Failed to initialize ds3 %d\n", rc);
		td_verror(td, rc, "ds3_init");
		return rc;
	}

	/* Connect to the DAOS pool */
	rc = ds3_connect(eo->pool, NULL, &ds3, NULL);
	if (rc) {
		log_err("Failed to connect to pool %d\n", rc);
		td_verror(td, rc, "ds3_connect");
		return rc;
	}

	/* Open the bucket */
	rc = ds3_bucket_open(eo->bucket, &bhdl, ds3, NULL);
	if (rc) {
		log_err("Failed to open bucket %s: %d\n", eo->bucket, rc);
		td_verror(td, rc, "ds3_bucket_open");
		return rc;
	}

	return 0;
}

static int ds3_fio_global_cleanup()
{
	int rc;
	int ret = 0;

	rc = ds3_disconnect(ds3, NULL);
	if (rc) {
		log_err("failed to disconnect pool: %d\n", rc);
		if (ret == 0)
			ret = rc;
	}
	rc = ds3_fini();
	if (rc) {
		log_err("failed to finalize ds3: %d\n", rc);
		if (ret == 0)
			ret = rc;
	}

	return ret;
}

static int ds3_fio_setup(struct thread_data *td)
{
	return 0;
}

static int ds3_fio_init(struct thread_data *td)
{
	struct daos_data	*dd;
	int			rc = 0;

	pthread_mutex_lock(&daos_mutex);

	dd = malloc(sizeof(*dd));
	if (dd == NULL) {
		log_err("Failed to allocate DAOS-private data\n");
		rc = ENOMEM;
		goto out;
	}

	dd->queued	= 0;
	dd->num_ios	= td->o.iodepth;
	dd->io_us	= calloc(dd->num_ios, sizeof(struct io_u *));
	if (dd->io_us == NULL) {
		log_err("Failed to allocate IO queue\n");
		rc = ENOMEM;
		goto out;
	}

	/* initialize DAOS stack if not already up */
	if (!ds3_initialized) {
		rc = ds3_fio_global_init(td);
		if (rc)
			goto out;
		ds3_initialized = true;
	}

	rc = daos_eq_create(&dd->eqh);
	if (rc) {
		log_err("Failed to create event queue: %d\n", rc);
		td_verror(td, rc, "daos_eq_create");
		goto out;
	}

	td->io_ops_data = dd;
	num_threads++;
out:
	if (rc) {
		if (dd) {
			free(dd->io_us);
			free(dd);
		}
		if (num_threads == 0 && ds3_initialized) {
			/* don't clobber error return value */
			(void)ds3_fio_global_cleanup();
			ds3_initialized = false;
		}
	}
	pthread_mutex_unlock(&daos_mutex);
	return rc;
}

static void ds3_fio_cleanup(struct thread_data *td)
{
	struct daos_data	*dd = td->io_ops_data;
	int			rc;

	if (dd == NULL)
		return;

	rc = daos_eq_destroy(dd->eqh, DAOS_EQ_DESTROY_FORCE);
	if (rc < 0) {
		log_err("failed to destroy event queue: %d\n", rc);
		td_verror(td, rc, "daos_eq_destroy");
	}

	free(dd->io_us);
	free(dd);

	pthread_mutex_lock(&daos_mutex);
	num_threads--;
	if (ds3_initialized && num_threads == 0) {
		int ret;

		ret = ds3_fio_global_cleanup();
		if (ret < 0 && rc == 0) {
			log_err("failed to clean up: %d\n", ret);
			td_verror(td, ret, "ds3_fio_global_cleanup");
		}
		ds3_initialized = false;
	}
	pthread_mutex_unlock(&daos_mutex);
}

static int ds3_fio_get_file_size(struct thread_data *td, struct fio_file *f)
{
	char		*file_name = f->file_name;
	struct daos_data	*dd = td->io_ops_data;
	struct ds3_object_info	info;
	int		rc;

	dprint(FD_FILE, "ds3 object info %s\n", file_name);

	if (!ds3_initialized)
		return 0;

	rc = ds3_obj_get_info(&info, bhdl, dd->obj);
	if (rc) {
		log_err("Failed to get info %s: %d\n", file_name, rc);
		td_verror(td, rc, "ds3_obj_get_info");
		return rc;
	}

	f->real_file_size = info.encoded_length;
	return 0;
}

static int ds3_fio_close(struct thread_data *td, struct fio_file *f)
{
	struct daos_data	*dd = td->io_ops_data;
	int			rc;

	dprint(FD_FILE, "ds3 close %s\n", f->file_name);

	rc = ds3_obj_close(dd->obj);
	if (rc) {
		log_err("Failed to close %s: %d\n", f->file_name, rc);
		td_verror(td, rc, "ds3_obj_close");
		return rc;
	}

	return 0;
}

static int ds3_fio_open(struct thread_data *td, struct fio_file *f)
{
	struct daos_data	*dd = td->io_ops_data;
	bool			create = false;
	int			rc;

	dprint(FD_FILE, "ds3 open %s (%s/%d/%d)\n",
	       f->file_name, td_write(td) && !read_only ? "rw" : "r",
	       td->o.create_on_open, td->o.allow_create);

	if (td->o.create_on_open && td->o.allow_create)
		create = true;

	if (td_write(td) && td->o.allow_create)
		create = true;
	rc = ds3_obj_open(f->file_name, &dd->obj, bhdl);
	if ((rc == -ENOENT) && create)
		rc = ds3_obj_create(f->file_name, &dd->obj, bhdl);
	if (rc) {
		log_err("Failed to open %s: %d\n", f->file_name, rc);
		td_verror(td, rc, "ds3_obj_open");
		return rc;
	}

	return 0;
}

static int ds3_fio_unlink(struct thread_data *td, struct fio_file *f)
{
	int rc;

	dprint(FD_FILE, "ds3 destroy %s\n", f->file_name);

	rc = ds3_obj_destroy(f->file_name, bhdl);
	if (rc) {
		log_err("Failed to destroy %s: %d\n", f->file_name, rc);
		td_verror(td, rc, "ds3_obj_destroy");
		return rc;
	}

	return 0;
}

static int ds3_fio_invalidate(struct thread_data *td, struct fio_file *f)
{
	dprint(FD_FILE, "ds3 invalidate %s\n", f->file_name);
	return 0;
}

static void ds3_fio_io_u_free(struct thread_data *td, struct io_u *io_u)
{
	struct daos_iou *io = io_u->engine_data;

	if (io) {
		io_u->engine_data = NULL;
		free(io);
	}
}

static int ds3_fio_io_u_init(struct thread_data *td, struct io_u *io_u)
{
	struct daos_iou *io;

	io = malloc(sizeof(struct daos_iou));
	if (!io) {
		td_verror(td, ENOMEM, "malloc");
		return ENOMEM;
	}
	io->io_u = io_u;
	io_u->engine_data = io;
	return 0;
}

static struct io_u * ds3_fio_event(struct thread_data *td, int event)
{
	struct daos_data *dd = td->io_ops_data;

	return dd->io_us[event];
}

static int ds3_fio_getevents(struct thread_data *td, unsigned int min,
			      unsigned int max, const struct timespec *t)
{
	struct daos_data	*dd = td->io_ops_data;
	daos_event_t		*evp[max];
	unsigned int		events = 0;
	int			i;
	int			rc;

	while (events < min) {
		rc = daos_eq_poll(dd->eqh, 0, DAOS_EQ_NOWAIT, max, evp);
		if (rc < 0) {
			log_err("Event poll failed: %d\n", rc);
			td_verror(td, rc, "daos_eq_poll");
			return events;
		}

		for (i = 0; i < rc; i++) {
			struct daos_iou	*io;
			struct io_u	*io_u;

			io = container_of(evp[i], struct daos_iou, ev);
			if (io->complete)
				log_err("Completion on already completed I/O\n");

			io_u = io->io_u;
			if (io->ev.ev_error)
				io_u->error = io->ev.ev_error;
			else
				io_u->resid = 0;

			dd->io_us[events] = io_u;
			dd->queued--;
			daos_event_fini(&io->ev);
			io->complete = true;
			events++;
		}
	}

	dprint(FD_IO, "ds3 eq_pool returning %d (%u/%u)\n", events, min, max);

	return events;
}

static enum fio_q_status ds3_fio_queue(struct thread_data *td,
					struct io_u *io_u)
{
	struct daos_data	*dd = td->io_ops_data;
	struct daos_iou		*io = io_u->engine_data;
	daos_off_t		offset = io_u->offset;
	int			rc;

	if (dd->queued == td->o.iodepth)
		return FIO_Q_BUSY;

	io->size = io_u->xfer_buflen;

	io->complete = false;
	rc = daos_event_init(&io->ev, dd->eqh, NULL);
	if (rc) {
		log_err("Event init failed: %d\n", rc);
		io_u->error = rc;
		return FIO_Q_COMPLETED;
	}

	switch (io_u->ddir) {
	case DDIR_WRITE:
		rc = ds3_obj_write(io_u->xfer_buf, offset, &io->size, bhdl, dd->obj, &io->ev);
		if (rc) {
			log_err("ds3_obj_write failed: %d\n", rc);
			io_u->error = rc;
			return FIO_Q_COMPLETED;
		}
		break;
	case DDIR_READ:
		rc = ds3_obj_read(io_u->xfer_buf, offset, &io->size, bhdl, dd->obj, &io->ev);
		if (rc) {
			log_err("ds3_obj_read failed: %d\n", rc);
			io_u->error = rc;
			return FIO_Q_COMPLETED;
		}
		break;
	case DDIR_SYNC:
		io_u->error = 0;
		return FIO_Q_COMPLETED;
	default:
		dprint(FD_IO, "Invalid IO type: %d\n", io_u->ddir);
		io_u->error = -DER_INVAL;
		return FIO_Q_COMPLETED;
	}

	dd->queued++;
	return FIO_Q_QUEUED;
}

static int ds3_fio_prep(struct thread_data fio_unused *td, struct io_u *io_u)
{
	return 0;
}

/* ioengine_ops for get_ioengine() */
FIO_STATIC struct ioengine_ops ioengine = {
	.name			= "ds3",
	.version		= FIO_IOOPS_VERSION,
	.flags			= FIO_DISKLESSIO | FIO_NODISKUTIL,

	.setup			= ds3_fio_setup,
	.init			= ds3_fio_init,
	.prep			= ds3_fio_prep,
	.cleanup		= ds3_fio_cleanup,

	.open_file		= ds3_fio_open,
	.invalidate		= ds3_fio_invalidate,
	.get_file_size		= ds3_fio_get_file_size,
	.close_file		= ds3_fio_close,
	.unlink_file		= ds3_fio_unlink,

	.queue			= ds3_fio_queue,
	.getevents		= ds3_fio_getevents,
	.event			= ds3_fio_event,
	.io_u_init		= ds3_fio_io_u_init,
	.io_u_free		= ds3_fio_io_u_free,

	.option_struct_size	= sizeof(struct ds3_fio_options),
	.options		= options,
};

static void fio_init fio_ds3_register(void)
{
	register_ioengine(&ioengine);
}

static void fio_exit fio_ds3_unregister(void)
{
	unregister_ioengine(&ioengine);
}
