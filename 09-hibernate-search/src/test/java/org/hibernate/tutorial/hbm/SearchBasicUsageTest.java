/*
 * Hibernate, Relational Persistence for Idiomatic Java
 *
 * Copyright (c) 2010, Red Hat Inc. or third-party contributors as
 * indicated by the @author tags or express copyright attribution
 * statements applied by the authors.  All third-party contributions are
 * distributed under license by Red Hat Inc.
 *
 * This copyrighted material is made available to anyone wishing to use, modify,
 * copy, or redistribute it subject to the terms and conditions of the GNU
 * Lesser General Public License, as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY
 * or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser General Public License
 * for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with this distribution; if not, write to:
 * Free Software Foundation, Inc.
 * 51 Franklin Street, Fifth Floor
 * Boston, MA  02110-1301  USA
 */
package org.hibernate.tutorial.hbm;

import java.util.Date;
import java.util.List;

import org.hibernate.*;
import org.hibernate.boot.MetadataSources;
import org.hibernate.boot.registry.StandardServiceRegistry;
import org.hibernate.boot.registry.StandardServiceRegistryBuilder;
import org.hibernate.search.FullTextSession;
import org.hibernate.search.Search;
import org.hibernate.search.query.dsl.QueryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import junit.framework.TestCase;

/**
 * Illustrates use of Hibernate search APIs.
 *
 * @author Giovanni Giorgi
 */
public class SearchBasicUsageTest extends TestCase {
	private SessionFactory sessionFactory;

	Logger logger=LoggerFactory.getLogger(getClass());
	@Override
	protected void setUp() throws Exception {
		// A SessionFactory is set up once for an application!
		final StandardServiceRegistry registry = new StandardServiceRegistryBuilder()
				.configure() // configures settings from hibernate.cfg.xml
				.build();
		try {
			sessionFactory = new MetadataSources( registry ).buildMetadata().buildSessionFactory();
		}
		catch (Exception e) {
			// The registry would be destroyed by the SessionFactory, but we had trouble building the SessionFactory
			// so destroy it manually.
			StandardServiceRegistryBuilder.destroy( registry );
			e.printStackTrace();
		}
	}

	@Override
	protected void tearDown() throws Exception {
		if ( sessionFactory != null ) {
			sessionFactory.close();
		}
	}


	@SuppressWarnings("unchecked")
	public void testBasicSearchUsage() throws InterruptedException {
		// create a couple of events...
		assertNotNull(sessionFactory);
		Session session = sessionFactory.openSession();
		
		FullTextSession fullTextSession = Search.getFullTextSession(session);
		fullTextSession.createIndexer().startAndWait();
		
		session.beginTransaction();
		session.save( new Event( "Our very first event!", new Date() ) );
		session.save( new Event( "A follow up event", new Date() ) );
		session.save( new Event( "JaVa rocks!", new Date() ) );
		session.save( new Event( "Daitan Razor  says: Java rocks!", new Date() ) );
		session.getTransaction().commit();
		session.close();

		// now lets pull events from the database and list them
		session = sessionFactory.openSession();
        session.beginTransaction();
        List result = session.createQuery( "from Event" ).list();
		for ( Event event : (List<Event>) result ) {
			System.out.println( "Event (" + event.getDate() + ") : " + event.getTitle() );
		}
        session.getTransaction().commit();
        
        priv_testSearchEngine();
        session.close();
	}
	
	public void priv_testSearchEngine() throws InterruptedException{
		assertNotNull(sessionFactory);
		Session session = sessionFactory.openSession();
		FullTextSession fullTextSession = Search.getFullTextSession(session);
		
		
		Transaction tx = fullTextSession.beginTransaction();

		// create native Lucene query using the query DSL
		// alternatively you can write the Lucene query using the Lucene query parser
		// or the Lucene programmatic API. The Hibernate Search DSL is recommended though
		QueryBuilder qb = fullTextSession.getSearchFactory()
		  .buildQueryBuilder().forEntity(Event.class).get();
		org.apache.lucene.search.Query query = qb
		  .keyword()
		  .onFields("title")
		  .matching("Java rocks!")
		  .createQuery();

		// wrap Lucene query in a org.hibernate.Query
		org.hibernate.Query hibQuery =
		    fullTextSession.createFullTextQuery(query, Event.class);

		// execute search
		@SuppressWarnings("rawtypes")
		List results = hibQuery.list();
		logger.info("Search results:#"+results.size());
		int c=0;
		for(Object r: results){
			logger.info((++c)+") "+r);
		}
		if(results.size()<2){
			fail("At least 2 record expected");
		}
		tx.commit();
		session.close();
	}
}
