use std::{
    io::{BufRead, BufReader, Read, Write},
    sync::Arc,
    time::Duration,
};

use color_eyre::{Report, Result};
use harriet::{
    triple_production::{RdfObject, RdfPredicate, RdfSubject, RdfTriple, TripleProducer},
    TurtleDocument,
};
use indicatif::ProgressStyle;
use lasso::{Spur, ThreadedRodeo};
use mimalloc::MiMalloc;
use rayon::iter::{ParallelBridge, ParallelIterator};
#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

fn main() -> Result<()> {
    let ent_ids: Arc<ThreadedRodeo<Spur>> = Arc::new(ThreadedRodeo::default());
    let rel_ids: Arc<ThreadedRodeo<Spur>> = Arc::new(ThreadedRodeo::default());
    let total_statements = rayon::scope(|scope| {
        let pb = indicatif::ProgressBar::new_spinner()
            .with_style(ProgressStyle::default_spinner().template("{spinner} {msg} {pos}")?);
        pb.set_message("parsing");
        pb.enable_steady_tick(Duration::from_millis(32));

        let (tx, rx) = crossbeam::channel::bounded(200);
        let (otx, orx) = crossbeam::channel::bounded(5000);

        {
            let pb = pb.clone();
            let ent_ids = ent_ids.clone();
            let rel_ids = rel_ids.clone();
            scope.spawn(move |_| {
                rx.into_iter().par_bridge().for_each(|buf: Box<[u8]>| {
                    let bufstr = unsafe { std::str::from_utf8_unchecked(buf.as_ref()) };
                    let doc = TurtleDocument::parse_full(bufstr).unwrap();
                    let triples = TripleProducer::produce_for_document(&doc).unwrap();
                    let mut interned = 0;
                    for triple in triples {
                        if let RdfTriple {
                            subject: RdfSubject::IRI(s),
                            predicate: RdfPredicate::IRI(p),
                            object: RdfObject::IRI(o),
                        } = triple
                        {
                            if let (Some(s), Some(o), Some(p)) = (
                                s.iri.strip_prefix("http://www.wikidata.org/entity/"),
                                o.iri.strip_prefix("http://www.wikidata.org/entity/"),
                                p.iri.strip_prefix("http://www.wikidata.org/prop/"),
                            ) {
                                let ssp = ent_ids.get_or_intern(s);
                                let sob = ent_ids.get_or_intern(o);
                                let spr = rel_ids.get_or_intern(p);
                                otx.send((ssp,sob,spr)).unwrap();
                                interned += 1;
                            }
                        }
                    }
                    pb.inc(interned);
                });
            });
        }
        {
            scope.spawn(move |_| {
                let of = std::fs::File::options().write(true).create(true).open("out.bt").unwrap();
                let mut bw = std::io::BufWriter::new(of);
                orx.into_iter().for_each(|(ssp, sob, spr)| {
                    // write as subj, pred, obj id triple
                    bw.write_all(&ssp.into_inner().get().to_be_bytes()).unwrap();
                    bw.write_all(&spr.into_inner().get().to_be_bytes()).unwrap();
                    bw.write_all(&sob.into_inner().get().to_be_bytes()).unwrap();
                });
            })
        }
        let mut bufdec = BufReader::new(std::io::stdin());
        let bsz = 32 * 1024;
        let mut buf: Vec<u8> = vec![0; bsz];
        loop {
            buf.resize(bsz, 0);
            let actual = bufdec.read(buf.as_mut_slice())?;
            if actual == 0 {
                break;
            }
            buf.truncate(actual);
            bufdec.read_until(b'\n', &mut buf)?;
            tx.send(buf.as_slice().into())?;
        }
        Ok::<_, Report>(pb.position())
    })?;
    eprintln!(
        "{} total ent-rel-ent statements, {} entity ids, {} relation ids",
        total_statements,
        ent_ids.len(),
        rel_ids.len()
    );
    let ent_ids = Arc::into_inner(ent_ids).unwrap();
    let rel_ids = Arc::into_inner(rel_ids).unwrap();
    serde_json::to_writer(std::fs::File::options().write(true).create(true).open("ent_ids.json")?, &ent_ids)?;
    serde_json::to_writer(std::fs::File::options().write(true).create(true).open("rel_ids.json")?, &rel_ids)?;

    Ok(())
}