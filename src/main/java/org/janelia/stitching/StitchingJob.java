package org.janelia.stitching;

import java.io.File;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;

import org.apache.commons.io.IOUtils;
import org.janelia.dataaccess.DataProvider;
import org.janelia.dataaccess.DataProviderFactory;
import org.janelia.dataaccess.DataProviderType;

import com.google.gson.Gson;

import mpicbg.stitching.StitchingParameters;

/**
 * Represents input parameters and customizations for tweaking the stitching/fusing procedure.
 *
 * @author Igor Pisarev
 */

public class StitchingJob implements Serializable {

	public enum PipelineStep
	{
		Metadata, // mandatory step
		Blur,
		Stitching,
		IntensityCorrection,
		Fusion,
		Export
	}

	private static final long serialVersionUID = 2619120742300093982L;

	private transient DataProvider dataProvider;
	private final DataProviderType dataProviderType;

	private final EnumSet< PipelineStep > pipeline;
	private StitchingArguments args;
	private transient StitchingParameters params;
	private final String baseFolder;

	private String saveFolder;
	private String datasetName;

	private List< TileInfo[] > tilesMultichannel;

	public StitchingJob( final StitchingArguments args )
	{
		this.args = args;

		dataProvider = DataProviderFactory.create( DataProviderFactory.detectType( args.inputTileConfigurations().get( 0 ) ) );
		dataProviderType = dataProvider.getType();

		pipeline = setUpPipeline( args );

		final File inputFile = new File( args.inputTileConfigurations().get( 0 ) ).getAbsoluteFile();
		baseFolder = saveFolder = inputFile.getParent();
		datasetName = inputFile.getName();
		if ( datasetName.endsWith( ".json" ) )
			datasetName = datasetName.substring( 0, datasetName.lastIndexOf( ".json" ) );
	}

	private EnumSet< PipelineStep > setUpPipeline( final StitchingArguments args )
	{
		final List< PipelineStep > pipelineStepsList = new ArrayList<>();

		// mandatory step that validates tile configurations and tries to add some missing tiles, etc.
		pipelineStepsList.add( PipelineStep.Metadata );

		if ( !args.fuseOnly() )
			pipelineStepsList.add( PipelineStep.Stitching );

		if ( !args.stitchOnly() )
			pipelineStepsList.add( PipelineStep.Fusion );

		return EnumSet.copyOf( pipelineStepsList );
	}

	public EnumSet< PipelineStep > getPipeline() { return pipeline; }

	public synchronized DataProvider getDataProvider()
	{
		if ( dataProvider == null )
			dataProvider = DataProviderFactory.create( dataProviderType );
		return dataProvider;
	}

	public StitchingArguments getArgs() { return args; }

	public StitchingParameters getParams() { return params; }
	public void setParams( final StitchingParameters params ) { this.params = params; }

	public int getChannels() {
		return args.inputTileConfigurations().size();
	}

	public TileInfo[] getTiles( final int channel ) {
		return tilesMultichannel.get( channel );
	}

	public int getMainChannelIndex() {
		return args.registrationChannelIndex() != null ? args.registrationChannelIndex() : 0;
	}

	public void setTiles( final TileInfo[] tiles, final int channel ) throws Exception {
		tilesMultichannel.set( channel, tiles );
		checkTilesConfiguration();
	}

	public void setTilesMultichannel( final List< TileInfo[] > tilesMultichannel ) throws Exception {
		this.tilesMultichannel = tilesMultichannel;
		checkTilesConfiguration();
	}

	public String getBaseFolder() { return baseFolder; }

	public String getSaveFolder() { return saveFolder; }
	public void setSaveFolder( final String saveFolder ) { this.saveFolder = saveFolder; }

	public String getDatasetName() { return datasetName; }

	public int getDimensionality() { return tilesMultichannel.get( 0 )[ 0 ].numDimensions(); }
	public double[] getPixelResolution() { return tilesMultichannel.get( 0 )[ 0 ].getPixelResolution(); }

	public void validateTiles() throws IOException
	{
		final int dimensionality = getDimensionality();

		double[] pixelResolution = getPixelResolution();
		if ( pixelResolution == null )
			pixelResolution = new double[] { 0.097, 0.097, 0.18 };

		for ( final TileInfo[] tiles : tilesMultichannel )
		{
			if ( tiles.length < 2 )
				throw new IllegalArgumentException( "There must be at least 2 tiles in the dataset" );

			for ( int i = 0; i < tiles.length; i++ )
				if ( tiles[ i ].getPosition().length != tiles[ i ].getSize().length )
					throw new IllegalArgumentException( "Incorrect dimensionality" );

			for ( int i = 1; i < tiles.length; i++ )
				if ( tiles[ i ].numDimensions() != tiles[ i - 1 ].numDimensions() )
					throw new IllegalArgumentException( "Incorrect dimensionality" );

			for ( final TileInfo tile : tiles )
				if ( tile.getPixelResolution() == null )
					tile.setPixelResolution( pixelResolution.clone() );

			if ( dimensionality != tiles[ 0 ].numDimensions() )
				throw new IllegalArgumentException( "Channels have different dimensionality" );
		}

		if ( !CheckTilesOnDisk.allTilesArePresent( dataProvider, tilesMultichannel ) )
			throw new IllegalArgumentException( "Cannot find some of the tile images on disk" );

		if ( params != null )
			params.dimensionality = dimensionality;
	}

	private void checkTilesConfiguration() throws Exception
	{
		for ( final TileInfo[] tiles : tilesMultichannel )
		{
			boolean malformed = ( tiles == null );
			if ( !malformed )
				for ( final TileInfo tile : tiles )
					if ( tile == null )
						malformed = true;

			if ( malformed )
				throw new NullPointerException( "Malformed input" );

			for ( int i = 0; i < tiles.length; i++ ) {
				if ( tiles[ i ].getFilePath() == null || tiles[ i ].getPosition() == null )
					throw new NullPointerException( "Some of required parameters are missing (file or position)" );

				if ( tiles[ i ].getIndex() == null )
					tiles[ i ].setIndex( i );
			}
		}
	}


	// TODO: pull request for making StitchingParameters serializable, then remove it
	private void writeObject( final ObjectOutputStream stream ) throws IOException {
		stream.defaultWriteObject();
		stream.write( new Gson().toJson( params ).getBytes() );
	}
	private void readObject( final ObjectInputStream stream ) throws IOException, ClassNotFoundException {
		stream.defaultReadObject();
		params = new Gson().fromJson( IOUtils.toString(stream), StitchingParameters.class );
	}
}
