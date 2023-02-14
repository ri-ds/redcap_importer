# Generated by Django 3.2.10 on 2023-02-14 16:47

from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    dependencies = [
        ('redcap_importer', '0008_remove_projectmetadata_date_last_downloaded_data_mongo'),
    ]

    operations = [
        migrations.AlterModelOptions(
            name='armmetadata',
            options={'ordering': ('arm_number',), 'verbose_name_plural': 'arm metadata'},
        ),
        migrations.AlterModelOptions(
            name='etllog',
            options={'ordering': ['-start_date'], 'verbose_name': 'ETL log'},
        ),
        migrations.AlterModelOptions(
            name='eventinstrumentmetadata',
            options={'verbose_name_plural': 'event instrument metadata'},
        ),
        migrations.AlterModelOptions(
            name='eventmetadata',
            options={'ordering': ('ordering',), 'verbose_name_plural': 'event metadata'},
        ),
        migrations.AlterModelOptions(
            name='fieldmetadata',
            options={'ordering': ['ordering'], 'verbose_name_plural': 'field metadata'},
        ),
        migrations.AlterModelOptions(
            name='instrumentmetadata',
            options={'verbose_name_plural': 'instrument metadata'},
        ),
        migrations.AlterModelOptions(
            name='projectmetadata',
            options={'verbose_name_plural': 'project metadata'},
        ),
        migrations.AlterModelOptions(
            name='redcapapiurl',
            options={'verbose_name': 'REDCap API URL'},
        ),
        migrations.AlterModelOptions(
            name='redcapconnection',
            options={'verbose_name': 'REDCap project connection'},
        ),
        migrations.AddField(
            model_name='etllog',
            name='file_name',
            field=models.TextField(blank=True, help_text='the file name if uploading a file', null=True),
        ),
        migrations.AlterField(
            model_name='etllog',
            name='redcap_project',
            field=models.CharField(max_length=255, verbose_name='REDCap project'),
        ),
        migrations.AlterField(
            model_name='redcapapiurl',
            name='url',
            field=models.URLField(help_text='write full path to api with ending backslash (ex. "https://redcap.research.cchmc.org/api/")', unique=True, verbose_name='URL'),
        ),
        migrations.AlterField(
            model_name='redcapconnection',
            name='api_url',
            field=models.ForeignKey(on_delete=django.db.models.deletion.PROTECT, to='redcap_importer.redcapapiurl', verbose_name='API URL'),
        ),
        migrations.AlterField(
            model_name='redcapconnection',
            name='unique_name',
            field=models.SlugField(help_text='A unique reference for this connection and associated Django app.It does not need to be the exact name of the REDCap project - that will be determined by the API key you specify in your settings file.', unique=True),
        ),
    ]