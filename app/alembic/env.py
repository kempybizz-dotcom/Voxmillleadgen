"""
from logging.config import fileConfig
from sqlalchemy import engine_from_config, pool
from alembic import context
import os
import sys

# Add parent directory to path
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

from app.config import settings
from app.database import Base
from app import models  # Import all models

config = context.config

# Set database URL from environment
config.set_main_option('sqlalchemy.url', settings.DATABASE_URL)

if config.config_file_name is not None:
    fileConfig(config.config_file_name)

target_metadata = Base.metadata


def run_migrations_offline() -> None:
    url = config.get_main_option("sqlalchemy.url")
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
    )

    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    connectable = engine_from_config(
        config.get_section(config.config_ini_section, {}),
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )

    with connectable.connect() as connection:
        context.configure(
            connection=connection, target_metadata=target_metadata
        )

        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
"""

# ============================================
# FILE: cli.py - Command Line Interface
# ============================================
"""
import click
import asyncio
from app.database import SessionLocal
from app.services.mining.lead_miner import LeadMiner
from app.services.intelligence.intel_processor import IntelligenceProcessor
from app import models
import csv
import sys


@click.group()
def cli():
    '''Voxmill CLI - Lead mining and management'''
    pass


@cli.command()
@click.option('--city', required=True, help='City to mine')
@click.option('--category', required=True, help='Business category')
@click.option('--country', default='UK', help='Country (UK/US)')
@click.option('--type', 'target_type', default='voxmill', type=click.Choice(['voxmill', 'agency']))
def mine(city, category, country, target_type):
    '''Mine leads for a specific city/category'''
    click.echo(f'Mining {category} in {city}, {country}...')
    
    miner = LeadMiner()
    processor = IntelligenceProcessor()
    
    # Mine leads
    raw_leads = asyncio.run(miner.mine_leads([category], [city], country, target_type))
    
    click.echo(f'Found {len(raw_leads)} leads. Processing intelligence...')
    
    # Save to database
    db = SessionLocal()
    saved = 0
    
    for raw_lead in raw_leads:
        try:
            # Check if exists
            existing = db.query(models.Lead).filter(
                models.Lead.place_id == raw_lead.get('place_id')
            ).first()
            
            if existing:
                click.echo(f'  ⚠️  {raw_lead["name"]} - Already exists')
                continue
            
            # Process
            enriched = asyncio.run(processor.process_lead(raw_lead, target_type))
            
            # Save
            lead = models.Lead(**{k: v for k, v in enriched.items() if k in models.Lead.__table__.columns.keys()})
            db.add(lead)
            db.flush()
            
            intel = models.Intelligence(
                lead_id=lead.id,
                critical_flaws=enriched.get('critical_flaws', ''),
                opportunity_angles=enriched.get('opportunity_angles', ''),
                outreach_hook=enriched.get('outreach_hook', ''),
                red_flags=enriched.get('red_flags', ''),
                pain_points=enriched.get('pain_points', ''),
                solution_angle=enriched.get('solution_angle', '')
            )
            db.add(intel)
            db.commit()
            
            saved += 1
            click.echo(f'  ✅ {lead.name} - Priority {lead.priority_score}')
            
        except Exception as e:
            click.echo(f'  ❌ Error: {e}')
            db.rollback()
    
    db.close()
    click.echo(f'\n✨ Complete! Saved {saved} new leads.')


@cli.command()
@click.option('--priority-min', default=7, help='Minimum priority score')
@click.option('--country', default=None, help='Filter by country')
@click.option('--output', default='leads.csv', help='Output file')
def export(priority_min, country, output):
    '''Export leads to CSV'''
    db = SessionLocal()
    
    query = db.query(models.Lead).filter(models.Lead.priority_score >= priority_min)
    
    if country:
        query = query.filter(models.Lead.country == country)
    
    leads = query.order_by(models.Lead.priority_score.desc()).all()
    
    if not leads:
        click.echo('No leads found matching criteria.')
        return
    
    # Write CSV
    with open(output, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        
        # Headers
        writer.writerow([
            'Name', 'Category', 'City', 'Country', 'Phone', 'Email', 'Website',
            'Rating', 'Reviews', 'Priority', 'Instagram', 'LinkedIn', 'Google Maps'
        ])
        
        # Data
        for lead in leads:
            writer.writerow([
                lead.name, lead.category, lead.city, lead.country,
                lead.phone, lead.email, lead.website,
                lead.rating, lead.total_reviews, lead.priority_score,
                lead.instagram_handle, lead.linkedin_url, lead.google_maps_url
            ])
    
    db.close()
    click.echo(f'✅ Exported {len(leads)} leads to {output}')


@cli.command()
@click.option('--days', default=7, help='Refresh leads older than N days')
def refresh(days):
    '''Refresh existing lead data'''
    from datetime import datetime, timedelta
    from app.worker import refresh_lead_data_task
    
    db = SessionLocal()
    cutoff = datetime.utcnow() - timedelta(days=days)
    
    leads = db.query(models.Lead).filter(
        (models.Lead.last_refreshed == None) | (models.Lead.last_refreshed < cutoff)
    ).all()
    
    click.echo(f'Queueing {len(leads)} leads for refresh...')
    
    for lead in leads:
        refresh_lead_data_task.delay(lead.id)
        click.echo(f'  📡 {lead.name}')
    
    db.close()
    click.echo('✅ Refresh jobs queued!')


@cli.command()
def stats():
    '''Show database statistics'''
    db = SessionLocal()
    
    total_leads = db.query(models.Lead).count()
    voxmill_leads = db.query(models.Lead).filter(models.Lead.category.in_([
        'Real Estate', 'Automotive', 'Hospitality', 'Premium Services'
    ])).count()
    agency_leads = total_leads - voxmill_leads
    
    high_priority = db.query(models.Lead).filter(models.Lead.priority_score >= 8).count()
    
    total_outreach = db.query(models.OutreachLog).count()
    replies = db.query(models.OutreachLog).filter(models.OutreachLog.replied == True).count()
    calls = db.query(models.OutreachLog).filter(models.OutreachLog.call_booked == True).count()
    deals = db.query(models.OutreachLog).filter(models.OutreachLog.deal_closed == True).count()
    
    click.echo('\n📊 VOXMILL DATABASE STATS\n')
    click.echo(f'Total Leads:        {total_leads}')
    click.echo(f'  Voxmill Targets:  {voxmill_leads}')
    click.echo(f'  Agency Targets:   {agency_leads}')
    click.echo(f'  High Priority:    {high_priority}')
    click.echo(f'\nOutreach Stats:')
    click.echo(f'  Messages Sent:    {total_outreach}')
    click.echo(f'  Replies:          {replies} ({replies/total_outreach*100:.1f}%)' if total_outreach > 0 else '  Replies:          0')
    click.echo(f'  Calls Booked:     {calls}')
    click.echo(f'  Deals Closed:     {deals}')
    
    db.close()


if __name__ == '__main__':
    cli()
"""
